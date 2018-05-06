package ClusterSchedulingSimulation.core


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
