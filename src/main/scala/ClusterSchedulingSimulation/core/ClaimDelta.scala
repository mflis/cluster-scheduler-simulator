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
