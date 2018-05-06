package ClusterSchedulingSimulation.omega

import ClusterSchedulingSimulation.core.{CellState, ClusterSimulator, Workload}

class OmegaSimulator(cellState: CellState,
                     override val schedulers: Map[String, OmegaScheduler],
                     workloadToSchedulerMap: Map[String, Seq[String]],
                     workloads: Seq[Workload],
                     prefillWorkloads: Seq[Workload],
                     logging: Boolean = false,
                     monitorUtilization: Boolean = true)
  extends ClusterSimulator(cellState,
    schedulers,
    workloadToSchedulerMap,
    workloads,
    prefillWorkloads,
    logging,
    monitorUtilization) {
  // Set up a pointer to this simulator in each scheduler.
  schedulers.values.foreach(_.omegaSimulator = this)
}
