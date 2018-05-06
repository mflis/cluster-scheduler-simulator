package ClusterSchedulingSimulation.core

class ClaimDelta(val scheduler: Scheduler,
                 val machineID: Int,
                 val machineSeqNum: Int,
                 val duration: Double,
                 val cpus: Double,
                 val mem: Double) {
  /**
    * Claim {@code cpus} and {@code mem} from {@code cellState}.
    * Increments the sequenceNum of the machine with ID referenced
    * by machineID.
    */
  def apply(cellState: CellState, locked: Boolean): Unit = {
    cellState.assignResources(scheduler, machineID, cpus, mem, locked)
    // Mark that the machine has changed, used for testing for conflicts
    // when using optimistic concurrency.
    cellState.machineSeqNums(machineID) += 1
  }

  def unApply(cellState: CellState, locked: Boolean = false): Unit = {
    cellState.freeResources(scheduler, machineID, cpus, mem, locked)
  }
}
