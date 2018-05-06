package ClusterSchedulingSimulation.core

/**
  * Generates jobs at a uniform rate, of a uniform size.
  */
class UniformWorkloadGenerator(val workloadName: String,
                               initJobInterarrivalTime: Double,
                               tasksPerJob: Int,
                               jobDuration: Double,
                               cpusPerTask: Double,
                               memPerTask: Double,
                               isRigid: Boolean = false)
  extends WorkloadGenerator {
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(timeWindow >= 0)
    val jobInterarrivalTime = updatedAvgJobInterarrivalTime
      .getOrElse(initJobInterarrivalTime)
    val workload = new Workload(workloadName)
    var nextJobSubmissionTime = 0.0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += jobInterarrivalTime
    }
    workload
  }

  def newJob(submissionTime: Double): Job = {
    Job(UniqueIDGenerator.getUniqueID(),
      submissionTime,
      tasksPerJob,
      jobDuration,
      workloadName,
      cpusPerTask,
      memPerTask,
      isRigid)
  }
}
