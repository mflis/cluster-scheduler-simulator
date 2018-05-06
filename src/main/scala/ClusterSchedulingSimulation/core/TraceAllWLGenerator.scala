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

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an emperical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from empirical distibutions
  * built from a prefill trace file. All tasks in a job are identical, so no
  * per-task data is required.
  */
class TraceAllWLGenerator(val workloadName: String,
                          interarrivalTraceFileName: String,
                          tasksPerJobTraceFileName: String,
                          jobDurationTraceFileName: String,
                          prefillTraceFileName: String,
                          maxCpusPerTask: Double,
                          maxMemPerTask: Double)
  extends WorkloadGenerator {
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val randomNumberGenerator = new util.Random(Seed())
  // Build the distributions from the input trace textfile that we'll
  // use to generate random job interarrival times.
  var interarrivalDist: Array[Double] =
  DistCache.getDistribution(workloadName, interarrivalTraceFileName)
  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)
  var jobDurationDist: Array[Double] =
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(timeWindow >= 0)
    assert(maxCpus == None)
    assert(maxMem == None)
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getQuantile(interarrivalDist,
      randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      nextJobSubmissionTime += updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        getQuantile(interarrivalDist,
          randomNumberGenerator.nextFloat)
      numJobs += 1
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0)
      dur = getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
    // Use ceil to avoid jobs with 0 tasks.
    val numTasks = math.ceil(getQuantile(tasksPerJobDist,
      randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, "Jobs must have at least one task.")
    // Sample from the empirical distribution until we get task
    // cpu and mem sizes that are small enough.
    var cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist,
      randomNumberGenerator.nextFloat).toFloat
    while (cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist,
        randomNumberGenerator.nextFloat).toFloat
    }
    var memPerTask = 0.7 * getQuantile(memPerTaskDist,
      randomNumberGenerator.nextFloat).toFloat
    while (memPerTask >= maxMemPerTask) {
      memPerTask = 0.7 * getQuantile(memPerTaskDist,
        randomNumberGenerator.nextFloat).toFloat
    }
    // println("cpusPerTask is %f, memPerTask %f.".format(cpusPerTask, memPerTask))
    Job(UniqueIDGenerator.getUniqueID(),
      submissionTime,
      numTasks,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask)
  }

  /**
    * @param value [0, 1] representing distribution quantile to return.
    */
  def getQuantile(empDistribution: Array[Double],
                  value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <= 1, "value must be >= 0 and <= 1.")
    val rawIndex = value * (empDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      return empDistribution(rawIndex.toInt)
    } else {
      val below = empDistribution(math.floor(rawIndex).toInt)
      val above = empDistribution(math.ceil(rawIndex).toInt)
      return below + interpAmount * (below + above)
    }
  }
}
