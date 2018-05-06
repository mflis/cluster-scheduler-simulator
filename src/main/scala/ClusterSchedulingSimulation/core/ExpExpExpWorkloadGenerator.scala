package ClusterSchedulingSimulation.core

import org.apache.commons.math.distribution.ExponentialDistributionImpl

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * interarrival rates, numTasks, and lengths sampled from exponential
  * distributions. Assumes that all tasks in a job are identical
  * (and so no per-task data is required).
  *
  * @param workloadName               the name that will be assigned to Workloads produced by
  *                                   this factory, and to the tasks they contain.
  * @param initAvgJobInterarrivalTime initial average inter-arrival time in
  *                                   seconds. This can be overriden by passing
  *                                   a non None value for
  *                                   updatedAvgJobInterarrivalTime to
  *                                   newWorkload().
  */
class ExpExpExpWorkloadGenerator(val workloadName: String,
                                 initAvgJobInterarrivalTime: Double,
                                 avgTasksPerJob: Double,
                                 avgJobDuration: Double,
                                 avgCpusPerTask: Double,
                                 avgMemPerTask: Double)
  extends WorkloadGenerator {
  val numTasksGenerator =
    new ExponentialDistributionImpl(avgTasksPerJob.toFloat)
  val durationGenerator = new ExponentialDistributionImpl(avgJobDuration)

  /**
    * Synchronized so that Experiments, which can share this WorkloadGenerator,
    * can safely call newWorkload concurrently.
    */
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(maxCpus == None)
    assert(maxMem == None)
    assert(timeWindow >= 0)
    // Create the job-interarrival-time number generator using the
    // parameter passed in, if any, else use the default parameter.
    val avgJobInterarrivalTime =
    updatedAvgJobInterarrivalTime.getOrElse(initAvgJobInterarrivalTime)
    var interarrivalTimeGenerator =
      new ExponentialDistributionImpl(avgJobInterarrivalTime)
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = interarrivalTimeGenerator.sample()
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += interarrivalTimeGenerator.sample()
    }
    workload
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = durationGenerator.sample()
    while (dur <= 0.0)
      dur = durationGenerator.sample()
    Job(UniqueIDGenerator.getUniqueID(),
      submissionTime,
      // Use ceil to avoid jobs with 0 tasks.
      math.ceil(numTasksGenerator.sample().toFloat).toInt,
      dur,
      workloadName,
      avgCpusPerTask,
      avgMemPerTask)
  }
}
