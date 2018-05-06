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


import scala.collection.mutable

class CellState(val numMachines: Int,
                val cpusPerMachine: Double,
                val memPerMachine: Double,
                val conflictMode: String,
                val transactionMode: String) {
  assert(conflictMode.equals("resource-fit") ||
    conflictMode.equals("sequence-numbers"),
    "conflictMode must be one of: {'resource-fit', 'sequence-numbers'}, " +
      "but it was %s.".format(conflictMode))
  assert(transactionMode.equals("all-or-nothing") ||
    transactionMode.equals("incremental"),
    "conflictMode must be one of: {'resource-fit', 'sequence-numbers'}, " +
      "but it was %s.".format(conflictMode))
  // An array where value at position k is the total cpus that have been
  // allocated for machine k.
  val allocatedCpusPerMachine = new Array[Double](numMachines)
  val allocatedMemPerMachine = new Array[Double](numMachines)
  val machineSeqNums = new Array[Int](numMachines)
  // Map from scheduler name to number of cpus assigned to that scheduler.
  val occupiedCpus = mutable.HashMap[String, Double]()
  val occupiedMem = mutable.HashMap[String, Double]()
  // Map from scheduler name to number of cpus locked while that scheduler
  // makes scheduling decisions about them, i.e., while resource offers made
  // to that scheduler containing those amount of resources is pending.
  val lockedCpus = mutable.HashMap[String, Double]()
  val lockedMem = mutable.HashMap[String, Double]()
  var simulator: ClusterSimulator = null
  // These used to be functions that sum the values of occupiedCpus,
  // occupiedMem, lockedCpus, and lockedMem, but its much faster to use
  // scalars that we update in assignResources and freeResources.
  var totalOccupiedCpus = 0.0
  var totalOccupiedMem = 0.0
  var totalLockedCpus = 0.0
  var totalLockedMem = 0.0

  /**
    * Allocate resources on a machine to a scheduler.
    *
    * @param locked Mark these resources as being pessimistically locked
    *               (i.e. while they are offered as part of a Mesos
    *               resource-offer).
    */
  def assignResources(scheduler: Scheduler,
                      machineID: Int,
                      cpus: Double,
                      mem: Double,
                      locked: Boolean) = {
    // Track the resources used by this scheduler.
    // assert(cpus <= availableCpus + 0.000001,
    //        ("Attempting to assign more CPUs (%f) than " +
    //         "are currently available in CellState (%f).")
    //        .format(cpus, availableCpus))
    // assert(mem <= availableMem + 0.000001,
    //        ("Attempting to assign more mem (%f) than " +
    //         "is currently available in CellState (%f).")
    //        .format(mem, availableMem))
    if (locked) {
      lockedCpus(scheduler.name) = lockedCpus.getOrElse(scheduler.name, 0.0) + cpus
      lockedMem(scheduler.name) = lockedMem.getOrElse(scheduler.name, 0.0) + mem
      assert(lockedCpus(scheduler.name) <= totalCpus + 0.000001)
      assert(lockedMem(scheduler.name) <= totalMem + 0.000001)
      totalLockedCpus += cpus
      totalLockedMem += mem
    } else {
      occupiedCpus(scheduler.name) = occupiedCpus.getOrElse(scheduler.name, 0.0) + cpus
      occupiedMem(scheduler.name) = occupiedMem.getOrElse(scheduler.name, 0.0) + mem
      assert(occupiedCpus(scheduler.name) <= totalCpus + 0.000001)
      assert(occupiedMem(scheduler.name) <= totalMem + 0.000001)
      totalOccupiedCpus += cpus
      totalOccupiedMem += mem
    }

    // Also track the per machine resources available.
    assert(availableCpusPerMachine(machineID) >= cpus,
      ("Scheduler %s (%d) tried to claim %f cpus on machine %d in cell " +
        "state %d, but it only has %f unallocated cpus right now.")
        .format(scheduler.name,
          scheduler.hashCode,
          cpus,
          machineID,
          hashCode(),
          availableCpusPerMachine(machineID)))
    assert(availableMemPerMachine(machineID) >= mem,
      ("Scheduler %s (%d) tried to claim %f mem on machine %d in cell " +
        "state %d, but it only has %f mem unallocated right now.")
        .format(scheduler.name,
          scheduler.hashCode,
          mem,
          machineID,
          hashCode(),
          availableMemPerMachine(machineID)))
    allocatedCpusPerMachine(machineID) += cpus
    allocatedMemPerMachine(machineID) += mem
  }

  // Convenience methods to see how many cpus/mem are available on a machine.
  def availableCpusPerMachine(machineID: Int) = {
    assert(machineID <= allocatedCpusPerMachine.length - 1,
      "There is no machine with ID %d.".format(machineID))
    cpusPerMachine - allocatedCpusPerMachine(machineID)
  }

  def availableMemPerMachine(machineID: Int) = {
    assert(machineID <= allocatedMemPerMachine.length - 1,
      "There is no machine with ID %d.".format(machineID))
    memPerMachine - allocatedMemPerMachine(machineID)
  }

  def totalCpus = numMachines * cpusPerMachine

  def totalMem = numMachines * memPerMachine

  // Release the specified number of resources used by this scheduler.
  def freeResources(scheduler: Scheduler,
                    machineID: Int,
                    cpus: Double,
                    mem: Double,
                    locked: Boolean) = {
    if (locked) {
      assert(lockedCpus.contains(scheduler.name))
      val safeToFreeCpus: Boolean = lockedCpus(scheduler.name) >= (cpus - 0.001)
      assert(safeToFreeCpus,
        "%s tried to free %f cpus, but was only occupying %f."
          .format(scheduler.name, cpus, lockedCpus(scheduler.name)))
      assert(lockedMem.contains(scheduler.name))
      val safeToFreeMem: Boolean = lockedMem(scheduler.name) >= (mem - 0.001)
      assert(safeToFreeMem,
        "%s tried to free %f mem, but was only occupying %f."
          .format(scheduler.name, mem, lockedMem(scheduler.name)))
      lockedCpus(scheduler.name) = lockedCpus(scheduler.name) - cpus
      lockedMem(scheduler.name) = lockedMem(scheduler.name) - mem
      totalLockedCpus -= cpus
      totalLockedMem -= mem
    } else {
      assert(occupiedCpus.contains(scheduler.name))
      val safeToFreeCpus: Boolean = occupiedCpus(scheduler.name) >= (cpus - 0.001)
      assert(safeToFreeCpus,
        "%s tried to free %f cpus, but was only occupying %f."
          .format(scheduler.name, cpus, occupiedCpus(scheduler.name)))
      assert(occupiedMem.contains(scheduler.name))
      val safeToFreeMem: Boolean = occupiedMem(scheduler.name) >= (mem - 0.001)
      assert(safeToFreeMem,
        "%s tried to free %f mem, but was only occupying %f."
          .format(scheduler.name, mem, occupiedMem(scheduler.name)))
      occupiedCpus(scheduler.name) = occupiedCpus(scheduler.name) - cpus
      occupiedMem(scheduler.name) = occupiedMem(scheduler.name) - mem
      totalOccupiedCpus -= cpus
      totalOccupiedMem -= mem
    }

    // Also track the per machine resources available.
    assert(availableCpusPerMachine(machineID) + cpus <=
      cpusPerMachine + 0.000001)
    assert(availableMemPerMachine(machineID) + mem <=
      memPerMachine + 0.000001)
    allocatedCpusPerMachine(machineID) -= cpus
    allocatedMemPerMachine(machineID) -= mem
  }

  /**
    * Return a copy of this cell state in its current state.
    */
  def copy: CellState = {
    val newCellState = new CellState(numMachines,
      cpusPerMachine,
      memPerMachine,
      conflictMode,
      transactionMode)
    Array.copy(src = allocatedCpusPerMachine,
      srcPos = 0,
      dest = newCellState.allocatedCpusPerMachine,
      destPos = 0,
      length = numMachines)
    Array.copy(src = allocatedMemPerMachine,
      srcPos = 0,
      dest = newCellState.allocatedMemPerMachine,
      destPos = 0,
      length = numMachines)
    Array.copy(src = machineSeqNums,
      srcPos = 0,
      dest = newCellState.machineSeqNums,
      destPos = 0,
      length = numMachines)
    newCellState.occupiedCpus ++= occupiedCpus
    newCellState.occupiedMem ++= occupiedMem
    newCellState.lockedCpus ++= lockedCpus
    newCellState.lockedMem ++= lockedMem
    newCellState.totalOccupiedCpus = totalOccupiedCpus
    newCellState.totalOccupiedMem = totalOccupiedMem
    newCellState.totalLockedCpus = totalLockedCpus
    newCellState.totalLockedMem = totalLockedMem
    newCellState
  }

  /**
    * Attempt to play the list of deltas, return any that conflicted.
    */
  def commit(deltas: Seq[ClaimDelta],
             scheduleEndEvent: Boolean = false): CommitResult = {
    var rollback = false // Track if we need to rollback changes.
    var appliedDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    var conflictDeltas = collection.mutable.ListBuffer[ClaimDelta]()

    def commitNonConflictingDeltas: Unit = {
      deltas.foreach(d => {
        if (causesConflict(d)) {
          simulator.log("delta (%s mach-%d seqNum-%d) caused a conflict."
            .format(d.scheduler, d.machineID, d.machineSeqNum))
          conflictDeltas += d
          if (transactionMode == "all-or-nothing") {
            rollback = true
            return
          } else if (transactionMode == "incremental") {

          } else {
            sys.error("Invalid transactionMode.")
          }
        } else {
          d.apply(cellState = this, locked = false)
          appliedDeltas += d
        }
      })
    }

    commitNonConflictingDeltas
    // Rollback if necessary.
    if (rollback) {
      simulator.log("Rolling back %d deltas.".format(appliedDeltas.length))
      appliedDeltas.foreach(d => {
        d.unApply(cellState = this, locked = false)
        conflictDeltas += d
        appliedDeltas -= d
      })
    }

    if (scheduleEndEvent) {
      scheduleEndEvents(appliedDeltas)
    }
    new CommitResult(appliedDeltas, conflictDeltas)
  }

  // Create an end event for each delta provided. The end event will
  // free the resources used by the task represented by the ClaimDelta.
  def scheduleEndEvents(claimDeltas: Seq[ClaimDelta]) {
    assert(simulator != null, "Simulator must be non-null in CellState.")
    claimDeltas.foreach(appliedDelta => {
      simulator.afterDelay(appliedDelta.duration) {
        appliedDelta.unApply(simulator.cellState)
        simulator.log(("A task started by scheduler %s finished. " +
          "Freeing %f cpus, %f mem. Available: %f cpus, %f mem.")
          .format(appliedDelta.scheduler.name,
            appliedDelta.cpus,
            appliedDelta.mem,
            availableCpus,
            availableMem))
      }
    })
  }

  def availableCpus = totalCpus - (totalOccupiedCpus + totalLockedCpus)

  def availableMem = totalMem - (totalOccupiedMem + totalLockedMem)

  /**
    * Tests if this delta causes a transaction conflict.
    * Different test scheme is used depending on conflictMode.
    */
  def causesConflict(delta: ClaimDelta): Boolean = {
    conflictMode match {
      case "sequence-numbers" => {
        // Use machine sequence numbers to test for conflicts.
        if (delta.machineSeqNum != machineSeqNums(delta.machineID)) {
          simulator.log("Sequence-number conflict occurred " +
            "(sched-%s, mach-%d, seq-num-%d, cpus-%f, mem-%f)."
              .format(delta.scheduler,
                delta.machineID,
                delta.machineSeqNum,
                delta.cpus,
                delta.mem))
          true
        } else {
          false
        }
      }
      case "resource-fit" => {
        // Check if the machine is currently short of resources,
        // regardless of whether sequence nums have changed.
        if (availableCpusPerMachine(delta.machineID) < delta.cpus ||
          availableMemPerMachine(delta.machineID) < delta.mem) {
          simulator.log("Resource-aware conflict occurred " +
            "(sched-%s, mach-%d, cpus-%f, mem-%f)."
              .format(delta.scheduler,
                delta.machineID,
                delta.cpus,
                delta.mem))
          true
        } else {
          false
        }
      }
      case _ => {
        sys.error("Unrecognized conflictMode %s.".format(conflictMode))
        true // Should never be reached.
      }
    }
  }

  case class CommitResult(committedDeltas: Seq[ClaimDelta],
                          conflictedDeltas: Seq[ClaimDelta])

}
