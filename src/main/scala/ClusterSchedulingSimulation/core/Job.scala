package ClusterSchedulingSimulation.core

/**
  * @param submitted the time the job was submitted in seconds
  * @param cpus      number of cpus required by this job
  * @param ram       amount of ram, in GB, required by this job
  */
case class Job(id: Long,
               submitted: Double,
               numTasks: Int,
               var taskDuration: Double,
               workloadName: String,
               cpusPerTask: Double,
               memPerTask: Double,
               isRigid: Boolean = false) {
  var unscheduledTasks: Int = numTasks
  // Time, in seconds, this job spent waiting in its scheduler's queue
  var timeInQueueTillFirstScheduled: Double = 0.0
  var timeInQueueTillFullyScheduled: Double = 0.0
  // Timestamp last inserted into scheduler's queue, used to compute timeInQueue.
  var lastEnqueued: Double = 0.0
  var lastSchedulingStartTime: Double = 0.0
  // Number of scheduling attempts for this job (regardless of how many
  // tasks were successfully scheduled in each attempt).
  var numSchedulingAttempts: Long = 0
  // Number of "task scheduling attempts". I.e. each time a scheduling
  // attempt is made for this job, we increase this counter by the number
  // of tasks that still need to be be scheduled (at the beginning of the
  // scheduling attempt). This is useful for analyzing the impact of no-fit
  // events on the scheduler busytime metric.
  var numTaskSchedulingAttempts: Long = 0
  var usefulTimeScheduling: Double = 0.0
  var wastedTimeScheduling: Double = 0.0

  def cpusStillNeeded: Double = cpusPerTask * unscheduledTasks

  def memStillNeeded: Double = memPerTask * unscheduledTasks

  // Calculate the maximum number of this jobs tasks that can fit into
  // the specified resources
  def numTasksToSchedule(cpusAvail: Double, memAvail: Double): Int = {
    if (cpusAvail == 0.0 || memAvail == 0.0) {
      return 0
    } else {
      val cpusChoppedToTaskSize = cpusAvail - (cpusAvail % cpusPerTask)
      val memChoppedToTaskSize = memAvail - (memAvail % memPerTask)
      val maxTasksThatWillFitByCpu = math.round(cpusChoppedToTaskSize / cpusPerTask)
      val maxTasksThatWillFitByMem = math.round(memChoppedToTaskSize / memPerTask)
      val maxTasksThatWillFit = math.min(maxTasksThatWillFitByCpu,
        maxTasksThatWillFitByMem)
      math.min(unscheduledTasks, maxTasksThatWillFit.toInt)
    }
  }

  def updateTimeInQueueStats(currentTime: Double) = {
    // Every time part of this job is partially scheduled, add to
    // the counter tracking how long it spends in the queue till
    // its final task is scheduled.
    timeInQueueTillFullyScheduled += currentTime - lastEnqueued
    // If this is the first scheduling done for this job, then make a note
    // about how long the job waited in the queue for this first scheduling.
    if (numSchedulingAttempts == 0) {
      timeInQueueTillFirstScheduled += currentTime - lastEnqueued
    }
  }
}
