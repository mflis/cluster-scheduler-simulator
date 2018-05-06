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

import scala.collection.mutable.ListBuffer

/**
  * A class that holds a list of jobs, each of which is used to record
  * statistics during a run of the simulator.
  *
  * Keep track of avgJobInterarrivalTime for easy reference later when
  * ExperimentRunner wants to record it in experiment result protos.
  */
class Workload(val name: String,
               private val jobs: ListBuffer[Job] = ListBuffer()) {
  def getJobs: Seq[Job] = jobs.toSeq

  def addJobs(jobs: Seq[Job]) = jobs.foreach(addJob)

  def numJobs: Int = jobs.length

  def cpus: Double = jobs.map(j => {
    j.numTasks * j.cpusPerTask
  }).sum

  def mem: Double = jobs.map(j => {
    j.numTasks * j.memPerTask
  }).sum

  // Generate a new workload that has a copy of the jobs that
  // this workload has.
  def copy: Workload = {
    val newWorkload = new Workload(name)
    jobs.foreach(job => {
      newWorkload.addJob(job.copy())
    })
    newWorkload
  }

  def addJob(job: Job) = {
    assert(job.workloadName == name)
    jobs.append(job)
  }

  def totalJobUsefulThinkTimes: Double = jobs.map(_.usefulTimeScheduling).sum

  def totalJobWastedThinkTimes: Double = jobs.map(_.wastedTimeScheduling).sum

  def avgJobInterarrivalTime: Double = {
    val submittedTimesArray = new Array[Double](jobs.length)
    jobs.map(_.submitted).copyToArray(submittedTimesArray)
    util.Sorting.quickSort(submittedTimesArray)
    // pass along (running avg, count)
    var sumInterarrivalTime = 0.0
    for (i <- 1 to submittedTimesArray.length - 1) {
      sumInterarrivalTime += submittedTimesArray(i) - submittedTimesArray(i - 1)
    }
    sumInterarrivalTime / submittedTimesArray.length
  }

  def jobUsefulThinkTimesPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduledJobs = jobs.filter(_.numSchedulingAttempts > 0).toList
    // println("Setting up thinkTimesArray of length " +
    //         scheduledJobs.length)
    if (scheduledJobs.length > 0) {
      val thinkTimesArray = new Array[Double](scheduledJobs.length)
      scheduledJobs.map(job => {
        job.usefulTimeScheduling
      }).copyToArray(thinkTimesArray)
      util.Sorting.quickSort(thinkTimesArray)
      //println(thinkTimesArray.deep.toSeq.mkString("-*-"))
      // println("Looking up think time percentile value at position " +
      //         ((thinkTimesArray.length-1) * percentile).toInt)
      thinkTimesArray(((thinkTimesArray.length - 1) * percentile).toInt)
    } else {
      -1.0
    }
  }

  def avgJobQueueTimeTillFirstScheduled: Double = {
    // println("Computing avgJobQueueTimeTillFirstScheduled.")
    val scheduledJobs = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduledJobs.length > 0) {
      val queueTimes = scheduledJobs.map(_.timeInQueueTillFirstScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def avgJobQueueTimeTillFullyScheduled: Double = {
    // println("Computing avgJobQueueTimeTillFullyScheduled.")
    val scheduledJobs = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduledJobs.length > 0) {
      val queueTimes = scheduledJobs.map(_.timeInQueueTillFullyScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def jobQueueTimeTillFirstScheduledPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduled = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduled.length > 0) {
      val queueTimesArray = new Array[Double](scheduled.length)
      scheduled.map(_.timeInQueueTillFirstScheduled)
        .copyToArray(queueTimesArray)
      util.Sorting.quickSort(queueTimesArray)
      val result =
        queueTimesArray(((queueTimesArray.length - 1) * percentile).toInt)
      println(("Looking up job queue time till first scheduled " +
        "percentile value at position %d of %d: %f.")
        .format(((queueTimesArray.length) * percentile).toInt,
          queueTimesArray.length,
          result))
      result
    } else {
      -1.0
    }
  }

  def jobQueueTimeTillFullyScheduledPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduled = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduled.length > 0) {
      val queueTimesArray = new Array[Double](scheduled.length)
      scheduled.map(_.timeInQueueTillFullyScheduled)
        .copyToArray(queueTimesArray)
      util.Sorting.quickSort(queueTimesArray)
      val result = queueTimesArray(((queueTimesArray.length - 1) * 0.9).toInt)
      println(("Looking up job queue time till fully scheduled " +
        "percentile value at position %d of %d: %f.")
        .format(((queueTimesArray.length) * percentile).toInt,
          queueTimesArray.length,
          result))
      result
    } else {
      -1.0
    }
  }

  def numSchedulingAttemptsPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    println("largest 200 job scheduling attempt counts: " +
      jobs.map(_.numSchedulingAttempts)
        .sorted
        .takeRight(200)
        .mkString(","))
    val scheduled = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduled.length > 0) {
      val schedulingAttemptsArray = new Array[Long](scheduled.length)
      scheduled.map(_.numSchedulingAttempts).copyToArray(schedulingAttemptsArray)
      util.Sorting.quickSort(schedulingAttemptsArray)
      val result = schedulingAttemptsArray(((schedulingAttemptsArray.length - 1) * 0.9).toInt)
      println(("Looking up num job scheduling attempts " +
        "percentile value at position %d of %d: %d.")
        .format(((schedulingAttemptsArray.length) * percentile).toInt,
          schedulingAttemptsArray.length,
          result))
      result
    } else {
      -1.0
    }
  }

  def numTaskSchedulingAttemptsPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    println("largest 200 task scheduling attempt counts: " +
      jobs.map(_.numTaskSchedulingAttempts)
        .sorted
        .takeRight(200)
        .mkString(","))
    val scheduled = jobs.filter(_.numTaskSchedulingAttempts > 0)
    if (scheduled.length > 0) {
      val taskSchedulingAttemptsArray = new Array[Long](scheduled.length)
      scheduled.map(_.numTaskSchedulingAttempts).copyToArray(taskSchedulingAttemptsArray)
      util.Sorting.quickSort(taskSchedulingAttemptsArray)
      val result = taskSchedulingAttemptsArray(((taskSchedulingAttemptsArray.length - 1) * 0.9).toInt)
      println(("Looking up num task scheduling attempts " +
        "percentile value at position %d of %d: %d.")
        .format(((taskSchedulingAttemptsArray.length) * percentile).toInt,
          taskSchedulingAttemptsArray.length,
          result))
      result
    } else {
      -1.0
    }
  }
}
