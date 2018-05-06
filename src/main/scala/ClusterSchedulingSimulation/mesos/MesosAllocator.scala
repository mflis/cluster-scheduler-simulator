package ClusterSchedulingSimulation.mesos

import ClusterSchedulingSimulation.core.ClaimDelta

import scala.collection.mutable

/**
  * Decides which scheduler to make resource offer to next, and manages
  * the resource offer process.
  *
  * @param constantThinkTime the time this scheduler takes to sort the
  *                          list of schedulers to decide which to offer to next. This happens
  *                          before each series of resource offers is made.
  * @param resources         How many resources is managed by this MesosAllocator
  */
class MesosAllocator(constantThinkTime: Double,
                     minCpuOffer: Double = 100.0,
                     minMemOffer: Double = 100.0,
                     // Min time, in seconds, to batch up resources
                     // before making an offer.
                     val offerBatchInterval: Double = 1.0) {
  val offeredDeltas = mutable.HashMap[Long, Seq[ClaimDelta]]()
  var simulator: MesosSimulator = null
  var allocating: Boolean = false
  var schedulersRequestingResources = collection.mutable.Set[MesosScheduler]()
  var timeSpentAllocating: Double = 0.0
  var nextOfferId: Long = 0
  // Are we currently waiting while a resource batch offer builds up
  // that has already been scheduled?
  var buildAndSendOfferScheduled = false

  def requestOffer(needySched: MesosScheduler) {
    checkRegistered
    simulator.log("Received an offerRequest from %s.".format(needySched.name))
    // Adding a scheduler to this list will ensure that it gets included
    // in the next round of resource offers.
    schedulersRequestingResources += needySched
    schedBuildAndSendOffer()
  }

  /**
    * We batch up available resources into periodic offers so
    * that we don't send an offer in response to *every* small event,
    * which adds latency to the average offer and slows the simulator down.
    * This feature was in Mesos for the NSDI paper experiments, but didn't
    * get committed to the open source codebase at that time.
    */
  def schedBuildAndSendOffer() = {
    if (!buildAndSendOfferScheduled) {
      buildAndSendOfferScheduled = true
      simulator.afterDelay(offerBatchInterval) {
        simulator.log("Building and sending a batched offer")
        buildAndSendOffer()
        // Let another call to buildAndSendOffer() get scheduled,
        // giving some time for resources to build up that are
        // becoming available due to tasks finishing.
        buildAndSendOfferScheduled = false
      }
    }
  }

  /**
    * Sort schedulers in simulator using DRF, then make an offer to
    * the first scheduler in the list.
    *
    * After any task finishes or scheduler says it wants offers, we
    * call this, i.e. buildAndSendOffer(), again. Note that the only
    * resources that will be available will be the ones that
    * the task that just finished was using).
    */
  def buildAndSendOffer(): Unit = {
    checkRegistered
    simulator.log("========================================================")
    simulator.log(("TOP OF BUILD AND SEND. CellState total occupied: " +
      "%fcpus (%.1f%%), %fmem (%.1f%%).")
      .format(simulator.cellState.totalOccupiedCpus,
        simulator.cellState.totalOccupiedCpus /
          simulator.cellState.totalCpus * 100.0,
        simulator.cellState.totalOccupiedMem,
        simulator.cellState.totalOccupiedMem /
          simulator.cellState.totalMem * 100.0))
    // Build and send an offer only if:
    // (a) there are enough resources in cellstate and
    // (b) at least one scheduler wants offers currently
    // Else, don't do anything, since this function will be called
    // again when a task finishes or a scheduler says it wants offers.
    if (!schedulersRequestingResources.isEmpty &&
      simulator.cellState.availableCpus >= minCpuOffer &&
      simulator.cellState.availableCpus >= minMemOffer) {
      // Use DRF to pick a candidate scheduler to offer resources.
      val sortedSchedulers =
        drfSortSchedulers(schedulersRequestingResources.toSeq)
      sortedSchedulers.headOption.foreach(candidateSched => {
        // Create an offer by taking a snapshot of cell state. We might
        // discard this without sending it if we find that there are
        // no available resources in cell state right now.
        val privCellState = simulator.cellState.copy
        val offer = Offer(nextOfferId, candidateSched, privCellState)
        nextOfferId += 1

        // Call scheduleAllAvailable() which creates deltas, applies them,
        // and returns them; all based on common cell state. This doesn't
        // affect the privateCellState we created above. Store the deltas
        // using the offerID as key until we get a response from the scheduler.
        // This has the effect of pessimistally locking the resources in
        // common cell state until we hear back from the scheduler (or time
        // out and rescind the offer).
        val claimDeltas =
        candidateSched.scheduleAllAvailable(cellState = simulator.cellState,
          locked = true)
        // Make sure scheduleAllAvailable() did its job.
        assert(simulator.cellState.availableCpus < 0.01 &&
          simulator.cellState.availableMem < 0.01,
          ("After scheduleAllAvailable() is called on a cell state " +
            "that cells state should not have any available resources " +
            "of any type, but this cell state still has %f cpus and %f " +
            "memory available").format(simulator.cellState.availableCpus,
            simulator.cellState.availableMem))
        if (!claimDeltas.isEmpty) {
          assert(privCellState.totalLockedCpus !=
            simulator.cellState.totalLockedCpus,
            "Since some resources were locked and put into a resource " +
              "offer, we expect the number of total lockedCpus to now be " +
              "different in the private cell state we created than in the" +
              "common cell state.")
          offeredDeltas(offer.id) = claimDeltas

          val thinkTime = getThinkTime
          simulator.afterDelay(thinkTime) {
            timeSpentAllocating += thinkTime
            simulator.log(("Allocator done thinking, sending offer to %s. " +
              "Offer contains private cell state with " +
              "%f cpu, %f mem available.")
              .format(candidateSched.name,
                offer.cellState.availableCpus,
                offer.cellState.availableMem))
            // Send the offer.
            candidateSched.resourceOffer(offer)
          }
        }
      })
    } else {
      var reason = ""
      if (schedulersRequestingResources.isEmpty)
        reason = "No schedulers currently want offers."
      if (simulator.cellState.availableCpus < minCpuOffer ||
        simulator.cellState.availableCpus < minMemOffer)
        reason = ("Only %f cpus and %f mem available in common cell state " +
          "but min offer size is %f cpus and %f mem.")
          .format(simulator.cellState.availableCpus,
            simulator.cellState.availableCpus,
            minCpuOffer,
            minMemOffer)
      simulator.log("Not sending an offer after all. %s".format(reason))
    }
  }

  def checkRegistered = {
    assert(simulator != null, "You must assign a simulator to a " +
      "MesosAllocator before you can use it.")
  }

  def getThinkTime: Double = {
    constantThinkTime
  }

  /**
    * 1/N multi-resource fair sharing.
    */
  def drfSortSchedulers(schedulers: Seq[MesosScheduler]): Seq[MesosScheduler] = {
    val schedulerDominantShares = schedulers.map(scheduler => {
      val shareOfCpus =
        simulator.cellState.occupiedCpus.getOrElse(scheduler.name, 0.0)
      val shareOfMem =
        simulator.cellState.occupiedMem.getOrElse(scheduler.name, 0.0)
      val domShare = math.max(shareOfCpus / simulator.cellState.totalCpus,
        shareOfMem / simulator.cellState.totalMem)
      var nameOfDomShare = ""
      if (shareOfCpus > shareOfMem) nameOfDomShare = "cpus"
      else nameOfDomShare = "mem"
      simulator.log("%s's dominant share is %s (%f%s)."
        .format(scheduler.name, nameOfDomShare, domShare, "%"))
      (scheduler, domShare)
    })
    schedulerDominantShares.sortBy(_._2).map(_._1)
  }

  def cancelOfferRequest(needySched: MesosScheduler) = {
    simulator.log("Canceling the outstanding resourceRequest for scheduler %s.".format(
      needySched.name))
    schedulersRequestingResources -= needySched
  }

  /**
    * Schedulers call this to respond to resource offers.
    */
  def respondToOffer(offer: Offer, claimDeltas: Seq[ClaimDelta]) = {
    checkRegistered
    simulator.log(("------Scheduler %s responded to offer %d with " +
      "%d claimDeltas.")
      .format(offer.scheduler.name, offer.id, claimDeltas.length))

    // Look up, unapply, & discard the saved deltas associated with the offerid.
    // This will cause the framework to stop being charged for the resources that
    // were locked while he made his scheduling decision.
    assert(offeredDeltas.contains(offer.id),
      "Allocator received response to offer that is not on record.")
    offeredDeltas.remove(offer.id).foreach(savedDeltas => {
      savedDeltas.foreach(_.unApply(cellState = simulator.cellState,
        locked = true))
    })
    simulator.log("========================================================")
    simulator.log("AFTER UNAPPLYING SAVED DELTAS")
    simulator.log("CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s)."
      .format(simulator.cellState.totalOccupiedCpus,
        simulator.cellState.totalOccupiedCpus /
          simulator.cellState.totalCpus * 100.0,
        "%",
        simulator.cellState.totalOccupiedMem,
        simulator.cellState.totalOccupiedMem /
          simulator.cellState.totalMem * 100.0,
        "%"))
    simulator.log("Committing all %d deltas that were part of response %d "
      .format(claimDeltas.length, offer.id))
    // commit() all deltas that were part of the offer response, don't use
    // the option of having cell state create the end events for us since we
    // want to add code to the end event that triggers another resource offer.
    if (claimDeltas.length > 0) {
      val commitResult = simulator.cellState.commit(claimDeltas, false)
      assert(commitResult.conflictedDeltas.length == 0,
        "Expecting no conflicts, but there were %d."
          .format(commitResult.conflictedDeltas.length))

      // Create end events for all tasks committed.
      commitResult.committedDeltas.foreach(delta => {
        simulator.afterDelay(delta.duration) {
          delta.unApply(simulator.cellState)
          simulator.log(("A task started by scheduler %s finished. " +
            "Freeing %f cpus, %f mem. Available: %f cpus, %f " +
            "mem. Also, triggering a new batched offer round.")
            .format(delta.scheduler.name,
              delta.cpus,
              delta.mem,
              simulator.cellState.availableCpus,
              simulator.cellState.availableMem))
          schedBuildAndSendOffer()
        }
      })
    }
    schedBuildAndSendOffer()
  }
}
