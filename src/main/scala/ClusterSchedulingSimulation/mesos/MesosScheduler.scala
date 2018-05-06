package ClusterSchedulingSimulation.mesos

import ClusterSchedulingSimulation.core.{ClaimDelta, Job, Scheduler}

class MesosScheduler(name: String,
                     constantThinkTimes: Map[String, Double],
                     perTaskThinkTimes: Map[String, Double],
                     val schedulePartialJobs: Boolean,
                     numMachinesToBlackList: Double = 0)
  extends Scheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList) {
  println("scheduler-id-info: %d, %s, %d, %s, %s"
    .format(Thread.currentThread().getId(),
      name,
      hashCode(),
      constantThinkTimes.mkString(";"),
      perTaskThinkTimes.mkString(";")))
  val offerQueue = new collection.mutable.Queue[Offer]
  // TODO(andyk): Clean up these <subclass>Simulator classes
  //              by templatizing the Scheduler class and having only
  //              one simulator of the correct type, instead of one
  //              simulator for each of the parent and child classes.
  var mesosSimulator: MesosSimulator = null

  override
  def checkRegistered = {
    super.checkRegistered
    assert(mesosSimulator != null, "This scheduler has not been added to a " +
      "simulator yet.")
  }

  /**
    * How an allocator sends offers to a framework.
    */
  def resourceOffer(offer: Offer): Unit = {
    offerQueue.enqueue(offer)
    handleNextResourceOffer()
  }

  def handleNextResourceOffer(): Unit = {
    // We essentially synchronize access to this scheduling logic
    // via the scheduling variable. We aren't protecting this from real
    // parallelism, but rather from discrete-event-simlation style parallelism.
    if (!scheduling && !offerQueue.isEmpty) {
      scheduling = true
      val offer = offerQueue.dequeue()
      // Use this offer to attempt to schedule jobs.
      simulator.log("------ In %s.resourceOffer(offer %d).".format(name, offer.id))
      val offerResponse = collection.mutable.ListBuffer[ClaimDelta]()
      var aggThinkTime: Double = 0.0
      // TODO(andyk): add an efficient method to CellState that allows us to
      //              check the largest slice of available resources to decode
      //              if we should keep trying to schedule or not.
      while (offer.cellState.availableCpus > 0.000001 &&
        offer.cellState.availableMem > 0.000001 &&
        !pendingQueue.isEmpty) {
        val job = pendingQueue.dequeue
        job.updateTimeInQueueStats(simulator.currentTime)
        val jobThinkTime = getThinkTime(job)
        aggThinkTime += jobThinkTime
        job.numSchedulingAttempts += 1
        job.numTaskSchedulingAttempts += job.unscheduledTasks

        // Before calling the expensive scheduleJob() function, check
        // to see if one of this job's tasks could fit into the sum of
        // *all* the currently free resources in the offers' cell state.
        // If one can't, then there is no need to call scheduleJob(). If
        // one can, we call scheduleJob(), though we still might not fit
        // any tasks due to fragmentation.
        if (offer.cellState.availableCpus > job.cpusPerTask &&
          offer.cellState.availableMem > job.cpusPerTask) {
          // Schedule the job using the cellstate in the ResourceOffer.
          val claimDeltas = scheduleJob(job, offer.cellState)
          if (claimDeltas.length > 0) {
            numSuccessfulTransactions += 1
            recordUsefulTimeScheduling(job,
              jobThinkTime,
              job.numSchedulingAttempts == 1)
            mesosSimulator.log(("Setting up job %d to accept at least " +
              "part of offer %d. About to spend %f seconds " +
              "scheduling it. Assigning %d tasks to it.")
              .format(job.id, offer.id, jobThinkTime,
                claimDeltas.length))
            offerResponse ++= claimDeltas
            job.unscheduledTasks -= claimDeltas.length
          } else {
            mesosSimulator.log(("Rejecting all of offer %d for job %d, " +
              "which requires tasks with %f cpu, %f mem. " +
              "Not counting busy time for this sched attempt.")
              .format(offer.id,
                job.id,
                job.cpusPerTask,
                job.memPerTask))
            numNoResourcesFoundSchedulingAttempts += 1
          }
        } else {
          mesosSimulator.log(("Short-path rejecting all of offer %d for " +
            "job %d because a single one of its tasks " +
            "(%f cpu, %f mem) wouldn't fit into the sum " +
            "of the offer's private cell state's " +
            "remaining resources (%f cpu, %f mem).")
            .format(offer.id,
              job.id,
              job.cpusPerTask,
              job.memPerTask,
              offer.cellState.availableCpus,
              offer.cellState.availableMem))
        }

        var jobEventType = "" // Set this conditionally below; used in logging.
        // If job is only partially scheduled, put it back in the pendingQueue.
        if (job.unscheduledTasks > 0) {
          mesosSimulator.log(("Job %d is [still] only partially scheduled, " +
            "(%d out of %d its tasks remain unscheduled) so " +
            "putting it back in the queue.")
            .format(job.id,
              job.unscheduledTasks,
              job.numTasks))
          // Give up on a job if (a) it hasn't scheduled a single task in
          // 100 tries or (b) it hasn't finished scheduling after 1000 tries.
          if ((job.numSchedulingAttempts > 100 &&
            job.unscheduledTasks == job.numTasks) ||
            job.numSchedulingAttempts > 1000) {
            println(("Abandoning job %d (%f cpu %f mem) with %d/%d " +
              "remaining tasks, after %d scheduling " +
              "attempts.").format(job.id,
              job.cpusPerTask,
              job.memPerTask,
              job.unscheduledTasks,
              job.numTasks,
              job.numSchedulingAttempts))
            numJobsTimedOutScheduling += 1
            jobEventType = "abandoned"
          } else {
            simulator.afterDelay(1) {
              addJob(job)
            }
          }
          job.lastEnqueued = simulator.currentTime
        } else {
          // All tasks in job scheduled so not putting it back in pendingQueue.
          jobEventType = "fully-scheduled"
        }
        if (!jobEventType.equals("")) {
          // Print some stats that we can use to generate CDFs of the job
          // # scheduling attempts and job-time-till-scheduled.
          // println("%s %s %d %s %d %d %f"
          //         .format(Thread.currentThread().getId(),
          //                 name,
          //                 hashCode(),
          //                 jobEventType,
          //                 job.id,
          //                 job.numSchedulingAttempts,
          //                 simulator.currentTime - job.submitted))
        }
      }

      if (pendingQueue.isEmpty) {
        // If we have scheduled everything, notify the allocator that we
        // don't need resources offers until we request them again (which
        // we will do when another job is added to our pendingQueue.
        // Do this before we reply to the offer since the allocator may make
        // its next round of offers shortly after we respond to this offer.
        mesosSimulator.log(("After scheduling, %s's pending queue is " +
          "empty, canceling outstanding " +
          "resource request.").format(name))
        mesosSimulator.allocator.cancelOfferRequest(this)
      } else {
        mesosSimulator.log(("%s's pending queue still has %d jobs in it, but " +
          "for some reason, they didn't fit into this " +
          "offer, so it will patiently wait for more " +
          "resource offers.").format(name, pendingQueue.size))
      }

      // Send our response to this offer.
      mesosSimulator.afterDelay(aggThinkTime) {
        mesosSimulator.log(("Waited %f seconds of aggThinkTime, now " +
          "responding to offer %d with %d responses after.")
          .format(aggThinkTime, offer.id, offerResponse.length))
        mesosSimulator.allocator.respondToOffer(offer, offerResponse)
      }
      // Done with this offer, see if we have another one to handle.
      scheduling = false
      handleNextResourceOffer()
    }
  }

  // When a job arrives, notify the allocator, so that it can make us offers
  // until we notify it that we don't have any more jobs, at which time it
  // can stop sending us offers.
  override
  def addJob(job: Job) = {
    assert(simulator != null, "This scheduler has not been added to a " +
      "simulator yet.")
    simulator.log("========================================================")
    simulator.log("addJOB: CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s)."
      .format(simulator.cellState.totalOccupiedCpus,
        simulator.cellState.totalOccupiedCpus /
          simulator.cellState.totalCpus * 100.0,
        "%",
        simulator.cellState.totalOccupiedMem,
        simulator.cellState.totalOccupiedMem /
          simulator.cellState.totalMem * 100.0,
        "%"))
    super.addJob(job)
    pendingQueue.enqueue(job)
    simulator.log("Enqueued job %d of workload type %s."
      .format(job.id, job.workloadName))
    mesosSimulator.allocator.requestOffer(this)
  }
}
