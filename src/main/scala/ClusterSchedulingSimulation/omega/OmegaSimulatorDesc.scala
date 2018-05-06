package ClusterSchedulingSimulation.omega

import ClusterSchedulingSimulation.core._

import scala.collection.mutable

class OmegaSimulatorDesc(
                          val schedulerDescs: Seq[OmegaSchedulerDesc],
                          runTime: Double,
                          val conflictMode: String,
                          val transactionMode: String)
  extends ClusterSimulatorDesc(runTime) {
  override
  def newSimulator(constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   blackListPercent: Double,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                   workloadToSchedulerMap: Map[String, Seq[String]],
                   cellStateDesc: CellStateDesc,
                   workloads: Seq[Workload],
                   prefillWorkloads: Seq[Workload],
                   logging: Boolean = false): ClusterSimulator = {
    assert(blackListPercent >= 0.0 && blackListPercent <= 1.0)
    var schedulers = mutable.HashMap[String, OmegaScheduler]()
    // Create schedulers according to experiment parameters.
    println("Creating %d schedulers.".format(schedulerDescs.length))
    schedulerDescs.foreach(schedDesc => {
      // If any of the scheduler-workload pairs we're sweeping over
      // are for this scheduler, then apply them before
      // registering it.
      var constantThinkTimes = mutable.HashMap[String, Double](
        schedDesc.constantThinkTimes.toSeq: _*)
      var perTaskThinkTimes = mutable.HashMap[String, Double](
        schedDesc.perTaskThinkTimes.toSeq: _*)
      var newBlackListPercent = 0.0
      if (schedulerWorkloadsToSweepOver
        .contains(schedDesc.name)) {
        newBlackListPercent = blackListPercent
        schedulerWorkloadsToSweepOver(schedDesc.name)
          .foreach(workloadName => {
            constantThinkTimes(workloadName) = constantThinkTime
            perTaskThinkTimes(workloadName) = perTaskThinkTime
          })
      }
      println("Creating new scheduler %s".format(schedDesc.name))
      schedulers(schedDesc.name) =
        new OmegaScheduler(schedDesc.name,
          constantThinkTimes.toMap,
          perTaskThinkTimes.toMap,
          math.floor(newBlackListPercent *
            cellStateDesc.numMachines.toDouble).toInt)
    })
    val cellState = new CellState(cellStateDesc.numMachines,
      cellStateDesc.cpusPerMachine,
      cellStateDesc.memPerMachine,
      conflictMode,
      transactionMode)
    println("Creating new OmegaSimulator with schedulers %s."
      .format(schedulers.values.map(_.toString).mkString(", ")))
    println("Setting OmegaSimulator(%s, %s)'s common cell state to %d"
      .format(conflictMode,
        transactionMode,
        cellState.hashCode))
    new OmegaSimulator(cellState,
      schedulers.toMap,
      workloadToSchedulerMap,
      workloads,
      prefillWorkloads,
      logging)
  }
}
