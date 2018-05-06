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

import ClusterSchedulingSimulation.Seed
import org.apache.commons.math.distribution.ExponentialDistributionImpl

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * sizes and lengths sampled from exponential distributions. Assumes that
  * all tasks in a job are identical, so no per-task data is required.
  * Generates interarrival times by sampling from an emperical distribution
  * built from a tracefile containing the interarrival times of jobs
  * in a real cluster.
  */
class InterarrivalTimeTraceExpExpWLGenerator(val workloadName: String,
                                             traceFileName: String,
                                             avgTasksPerJob: Double,
                                             avgJobDuration: Double,
                                             avgCpusPerTask: Double,
                                             avgMemPerTask: Double,
                                             maxCpusPerTask: Double,
                                             maxMemPerTask: Double)
  extends WorkloadGenerator {
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  // Build the distribution from the input trace textfile that we'll
  // use to generate random job interarrival times.
  val interarrivalTimes = new collection.mutable.ListBuffer[Double]()
  val numTasksGenerator =
    new ExponentialDistributionImpl(avgTasksPerJob.toFloat)
  val durationGenerator = new ExponentialDistributionImpl(avgJobDuration)
  val cpusPerTaskGenerator =
    new ExponentialDistributionImpl(avgCpusPerTask)
  val memPerTaskGenerator = new ExponentialDistributionImpl(avgMemPerTask)
  val randomNumberGenerator = new util.Random(Seed())
  var refDistribution: Array[Double] =
    DistCache.getDistribution(workloadName, traceFileName)

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime == None)
    assert(timeWindow >= 0)
    assert(maxCpus == None)
    assert(maxMem == None)
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getInterarrivalTime(randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += getInterarrivalTime(randomNumberGenerator.nextFloat)
      numJobs += 1
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  /**
    * @param value quantile [0, 1] representing distribution quantile to return.
    */
  def getInterarrivalTime(value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <= 1, "value must be >= 0 and <= 1.")
    val rawIndex = value * (refDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      return refDistribution(rawIndex.toInt)
    } else {
      val below = refDistribution(math.floor(rawIndex).toInt)
      val above = refDistribution(math.ceil(rawIndex).toInt)
      return below + interpAmount * (below + above)
    }
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = durationGenerator.sample()
    while (dur <= 0.0)
      dur = durationGenerator.sample()
    // Sample until we get task cpu and mem sizes that are small enough.
    var cpusPerTask = cpusPerTaskGenerator.sample()
    while (cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = cpusPerTaskGenerator.sample()
    }
    var memPerTask = memPerTaskGenerator.sample()
    while (memPerTask >= maxMemPerTask) {
      memPerTask = memPerTaskGenerator.sample()
    }
    Job(UniqueIDGenerator.getUniqueID(),
      submissionTime,
      // Use ceil to avoid jobs with 0 tasks.
      math.ceil(numTasksGenerator.sample().toFloat).toInt,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask)
  }
}
