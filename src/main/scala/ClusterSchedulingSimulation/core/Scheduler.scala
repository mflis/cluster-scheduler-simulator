package ClusterSchedulingSimulation.core

import ClusterSchedulingSimulation.Seed

import scala.collection.mutable


/**
  * Copyright (c) 2013, Regents of the University of California
  * All rights reserved.
  *
  * Redistribution and use in source and binary forms, with or without
  * modification, are permitted provided that the following conditions are met:
  *
  * Redistributions of source code must retain the above copyright notice, this
  * list of conditions and the following disclaimer.  Redistributions in binary
  * form must reproduce the above copyright notice, this list of conditions and the
  * following disclaimer in the documentation and/or other materials provided with
  * the distribution.  Neither the name of the University of California, Berkeley
  * nor the names of its contributors may be used to endorse or promote products
  * derived from this software without specific prior written permission.  THIS
  * SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
  * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */


/**
  * A Scheduler maintains {@code Job}s submitted to it in a queue, and
  * attempts to match those jobs with resources by making "job scheduling
  * decisions", which take a certain amount of "scheduling time".
  * A simulator is responsible for interacting with a Scheduler, e.g.,
  * by deciding which workload types should be assigned to which scheduler.
  * A Scheduler must accept Jobs associated with any workload type (workloads
  * are identified by name), though it can do whatever it wants with those
  * jobs, include, optionally, dropping them on the floor, or handling jobs
  * from different workloads differently, which concretely, means taking
  * a different amount of "scheuduling time" to schedule jobs from different
  * workloads.
  *
  * @param simulator             The simulator this scheduler is running inside of.
  * @param name                  Unique name that this scheduler is known by for the purposes
  *                              of jobs being assigned to.
  * @param contstantThinkTimes   Map from workloadNames to constant times,
  *                              in seconds, this scheduler uses to schedule each job.
  * @param perTaskThinkTimes     Map from workloadNames to times, in seconds,
  *                              this scheduler uses to schedule each task that is assigned to
  *                              a scheduler of that name.
  * @param numMachineToBlackList a positive number representing how many
  *                              machines (chosen randomly) this scheduler should ignore when
  *                              making scheduling decisions.
  */
abstract class Scheduler(val name: String,
                         constantThinkTimes: Map[String, Double],
                         perTaskThinkTimes: Map[String, Double],
                         numMachinesToBlackList: Double) {
  assert(constantThinkTimes.size == perTaskThinkTimes.size)
  assert(numMachinesToBlackList >= 0)
  // Keep a cache of candidate pools around, indexed by their length
  // to avoid the overhead of the Array.range call in our inner scheduling
  // loop.
  val candidatePoolCache = mutable.HashMap[Int, mutable.IndexedSeq[Int]]()
  val randomNumberGenerator = new util.Random(Seed())
  // This gets set when this scheduler is added to a Simulator.
  // TODO(andyk): eliminate this pointer and make the scheduler
  //              more functional.
  // TODO(andyk): Clean up these <subclass>Simulator classes
  //              by templatizing the Scheduler class and having only
  //              one simulator of the correct type, instead of one
  //              simulator for each of the parent and child classes.
  var simulator: ClusterSimulator = null
  var scheduling: Boolean = false
  // Job transaction stat counters.
  var numSuccessfulTransactions: Int = 0
  var numFailedTransactions: Int = 0
  var numRetriedTransactions: Int = 0
  var dailySuccessTransactions = mutable.HashMap[Int, Int]()
  var dailyFailedTransactions = mutable.HashMap[Int, Int]()
  var numJobsTimedOutScheduling: Int = 0
  // Task transaction stat counters.
  var numSuccessfulTaskTransactions: Int = 0
  var numFailedTaskTransactions: Int = 0
  var numNoResourcesFoundSchedulingAttempts: Int = 0
  // When trying to place a task, count the number of machines we look at
  // that the task doesn't fit on. This is a sort of wasted work that
  // causes the simulation to go slow.
  var failedFindVictimAttempts: Int = 0
  var totalUsefulTimeScheduling = 0.0 // in seconds
  var totalWastedTimeScheduling = 0.0 // in seconds
  var firstAttemptUsefulTimeScheduling = 0.0 // in seconds
  var firstAttemptWastedTimeScheduling = 0.0 // in seconds
  var dailyUsefulTimeScheduling = mutable.HashMap[Int, Double]()
  var dailyWastedTimeScheduling = mutable.HashMap[Int, Double]()
  // Also track the time, in seconds, spent scheduling broken out by
  // workload type. Note that all Schedulers (even, e.g., SinglePath
  // schedulers) can handle jobs from multiple workload generators.
  var perWorkloadUsefulTimeScheduling = mutable.HashMap[String, Double]()
  var perWorkloadWastedTimeScheduling = mutable.HashMap[String, Double]()
  protected var pendingQueue = new collection.mutable.Queue[Job]

  override
  def toString = {
    name
  }

  def checkRegistered = {
    assert(simulator != null, "You must assign a simulator to a " +
      "Scheduler before you can use it.")
  }


  // Add a job to this scheduler's job queue.
  def addJob(job: Job): Unit = {
    // Make sure the perWorkloadTimeSchuduling Map has a key for this job's
    // workload type, so that we still print something for statistics about
    // that workload type for this scheduler, even if this scheduler never
    // actually gets a chance to schedule a job of that type.
    perWorkloadUsefulTimeScheduling(job.workloadName) =
      perWorkloadUsefulTimeScheduling.getOrElse(job.workloadName, 0.0)
    perWorkloadWastedTimeScheduling(job.workloadName) =
      perWorkloadWastedTimeScheduling.getOrElse(job.workloadName, 0.0)
    job.lastEnqueued = simulator.currentTime
  }

  /**
    * Creates and applies ClaimDeltas for all available resources in the
    * provided {@code cellState}. This is intended to leave no resources
    * free in cellState, thus it doesn't use minCpu or minMem because that
    * could lead to leaving fragmentation. I haven't thought through
    * very carefully if floating point math could cause a problem here.
    */
  def scheduleAllAvailable(cellState: CellState,
                           locked: Boolean): Seq[ClaimDelta] = {
    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    for (mID <- 0 to cellState.numMachines - 1) {
      val cpusAvail = cellState.availableCpusPerMachine(mID)
      val memAvail = cellState.availableMemPerMachine(mID)
      if (cpusAvail > 0.0 || memAvail > 0.0) {
        // Create and apply a claim delta.
        assert(mID >= 0 && mID < cellState.machineSeqNums.length)
        //TODO(andyk): Clean up semantics around taskDuration in ClaimDelta
        //             since we want to represent offered resources, not
        //             tasks with these deltas.
        val claimDelta = new ClaimDelta(this,
          mID,
          cellState.machineSeqNums(mID),
          -1.0,
          cpusAvail,
          memAvail)
        claimDelta.apply(cellState, locked)
        claimDeltas += claimDelta
      }
    }
    claimDeltas
  }

  /**
    * Given a job and a cellstate, find machines that the tasks of the
    * job will fit into, and allocate the resources on that machine to
    * those tasks, accounting those resoures to this scheduler, modifying
    * the provided cellstate (by calling apply() on the created deltas).
    *
    * Implements the following randomized first fit scheduling algorithm:
    * while(more machines in candidate pool and more tasks to schedule):
    * candidateMachine = random machine in pool
    * if(candidate machine can hold at least one of this jobs tasks):
    * create a delta assigning the task to that machine
    * else:
    * remove from candidate pool
    *
    * @param  machineBlackList an array of of machineIDs that should be
    *                          ignored when scheduling (i.e. tasks will
    *                          not be scheduled on any of them).
    * @return List of deltas, one per task, so that the transactions can
    *         be played on some other cellstate if desired.
    */
  def scheduleJob(job: Job,
                  cellState: CellState): Seq[ClaimDelta] = {
    assert(simulator != null)
    assert(cellState != null)
    assert(job.cpusPerTask <= cellState.cpusPerMachine,
      "Looking for machine with %f cpus, but machines only have %f cpus."
        .format(job.cpusPerTask, cellState.cpusPerMachine))
    assert(job.memPerTask <= cellState.memPerMachine,
      "Looking for machine with %f mem, but machines only have %f mem."
        .format(job.memPerTask, cellState.memPerMachine))
    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()

    // Cache candidate pools in this scheduler for performance improvements.
    val candidatePool =
      candidatePoolCache.getOrElseUpdate(cellState.numMachines,
        Array.range(0, cellState.numMachines))

    var numRemainingTasks = job.unscheduledTasks
    var remainingCandidates =
      math.max(0, cellState.numMachines - numMachinesToBlackList).toInt
    while (numRemainingTasks > 0 && remainingCandidates > 0) {
      // Pick a random machine out of the remaining pool, i.e., out of the set
      // of machineIDs in the first remainingCandidate slots of the candidate
      // pool.
      val candidateIndex = randomNumberGenerator.nextInt(remainingCandidates)
      val currMachID = candidatePool(candidateIndex)

      // If one of this job's tasks will fit on this machine, then assign
      // to it by creating a claimDelta and leave it in the candidate pool.
      if (cellState.availableCpusPerMachine(currMachID) >= job.cpusPerTask &&
        cellState.availableMemPerMachine(currMachID) >= job.memPerTask) {
        assert(currMachID >= 0 && currMachID < cellState.machineSeqNums.length)
        val claimDelta = new ClaimDelta(this,
          currMachID,
          cellState.machineSeqNums(currMachID),
          job.taskDuration,
          job.cpusPerTask,
          job.memPerTask)
        claimDelta.apply(cellState = cellState, locked = false)
        claimDeltas += claimDelta
        numRemainingTasks -= 1
      } else {
        failedFindVictimAttempts += 1
        // Move the chosen candidate to the end of the range of
        // remainingCandidates so that we won't choose it again after we
        // decrement remainingCandidates. Do this by swapping it with the
        // machineID currently at position (remainingCandidates - 1)
        candidatePool(candidateIndex) = candidatePool(remainingCandidates - 1)
        candidatePool(remainingCandidates - 1) = currMachID
        remainingCandidates -= 1
        // simulator.log(
        //     ("%s in scheduling algorithm, tried machine %d, but " +
        //      "%f cpus and %f mem are required, and it only " +
        //      "has %f cpus and %f mem available.")
        //      .format(name,
        //              currMachID,
        //              job.cpusPerTask,
        //              job.memPerTask,
        //              cellState.availableCpusPerMachine(currMachID),
        //              cellState.availableMemPerMachine(currMachID)))
      }
    }
    return claimDeltas

  }

  def jobQueueSize: Long = pendingQueue.size

  def isMultiPath: Boolean =
    constantThinkTimes.values.toSet.size > 1 ||
      perTaskThinkTimes.values.toSet.size > 1

  def recordUsefulTimeScheduling(job: Job,
                                 timeScheduling: Double,
                                 isFirstSchedAttempt: Boolean): Unit = {
    assert(simulator != null, "This scheduler has not been added to a " +
      "simulator yet.")
    // Scheduler level stats.
    totalUsefulTimeScheduling += timeScheduling
    addDailyTimeScheduling(dailyUsefulTimeScheduling, timeScheduling)
    if (isFirstSchedAttempt) {
      firstAttemptUsefulTimeScheduling += timeScheduling
    }
    simulator.log("Recorded %f seconds of %s useful think time, total now: %f."
      .format(timeScheduling, name, totalUsefulTimeScheduling))
    // Job/workload level stats.
    job.usefulTimeScheduling += timeScheduling
    simulator.log("Recorded %f seconds of job %s useful think time, total now: %f."
      .format(timeScheduling, job.id, simulator.workloads.filter(_.name == job.workloadName).head.totalJobUsefulThinkTimes))
    // Also track per-path (i.e., per workload) scheduling times
    perWorkloadUsefulTimeScheduling(job.workloadName) =
      perWorkloadUsefulTimeScheduling.getOrElse(job.workloadName, 0.0) +
        timeScheduling
  }

  def addDailyTimeScheduling(counter: mutable.HashMap[Int, Double],
                             timeScheduling: Double) = {
    val index: Int = math.floor(simulator.currentTime / 86400).toInt
    val currAmt: Double = counter.getOrElse(index, 0.0)
    counter(index) = currAmt + timeScheduling
  }

  def recordWastedTimeScheduling(job: Job,
                                 timeScheduling: Double,
                                 isFirstSchedAttempt: Boolean): Unit = {
    assert(simulator != null, "This scheduler has not been added to a " +
      "simulator yet.")
    // Scheduler level stats.
    totalWastedTimeScheduling += timeScheduling
    addDailyTimeScheduling(dailyWastedTimeScheduling, timeScheduling)
    if (isFirstSchedAttempt) {
      firstAttemptWastedTimeScheduling += timeScheduling
    }
    // Job/workload level stats.
    job.wastedTimeScheduling += timeScheduling
    // Also track per-path (i.e., per workload) scheduling times
    perWorkloadWastedTimeScheduling(job.workloadName) =
      perWorkloadWastedTimeScheduling.getOrElse(job.workloadName, 0.0) +
        timeScheduling
  }

  /**
    * Computes the time, in seconds, this scheduler requires to make
    * a scheduling decision for {@code job}.
    *
    * @param job the job to determine this schedulers think time for
    */
  def getThinkTime(job: Job): Double = {
    assert(constantThinkTimes.contains(job.workloadName))
    assert(perTaskThinkTimes.contains(job.workloadName))
    constantThinkTimes(job.workloadName) +
      perTaskThinkTimes(job.workloadName) * job.unscheduledTasks.toFloat
  }
}
