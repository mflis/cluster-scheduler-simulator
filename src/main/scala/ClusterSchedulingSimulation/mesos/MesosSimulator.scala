package ClusterSchedulingSimulation.mesos

import ClusterSchedulingSimulation.core.{CellState, ClusterSimulator, Workload}

class MesosSimulator(cellState: CellState,
                     override val schedulers: Map[String, MesosScheduler],
                     workloadToSchedulerMap: Map[String, Seq[String]],
                     workloads: Seq[Workload],
                     prefillWorkloads: Seq[Workload],
                     var allocator: MesosAllocator,
                     logging: Boolean = false,
                     monitorUtilization: Boolean = true)
  extends ClusterSimulator(cellState,
    schedulers,
    workloadToSchedulerMap,
    workloads,
    prefillWorkloads,
    logging,
    monitorUtilization) {
  assert(cellState.conflictMode.equals("resource-fit"),
    "Mesos requires cellstate to be set up with resource-fit conflictMode")
  // Set up a pointer to this simulator in the allocator.
  allocator.simulator = this

  log("========================================================")
  log("Mesos SIM CONSTRUCTOR - CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s)."
    .format(cellState.totalOccupiedCpus,
      cellState.totalOccupiedCpus /
        cellState.totalCpus * 100.0,
      "%",
      cellState.totalOccupiedMem,
      cellState.totalOccupiedMem /
        cellState.totalMem * 100.0,
      "%"))

  // Set up a pointer to this simulator in each scheduler.
  schedulers.values.foreach(_.mesosSimulator = this)
}
