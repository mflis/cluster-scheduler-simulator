package ClusterSchedulingSimulation.core

/**
  * A threadsafe Workload factory.
  */
trait WorkloadGenerator {
  val workloadName: String

  /**
    * Generate a workload using this workloadGenerator's parameters
    * The timestamps of the jobs in this workload should fall in the range
    * [0, timeWindow].
    *
    * @param maxCpus                       The maximum number of cpus that the workload returned
    *                                      can contain.
    * @param maxMem                        The maximum amount of mem that the workload returned
    *                                      can contain.
    * @param updatedAvgJobInterarrivalTime if non-None, then the interarrival
    *                                      times of jobs in the workload returned
    *                                      will be approximately this value.
    */
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload
}
