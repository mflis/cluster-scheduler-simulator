package ClusterSchedulingSimulation.mesos


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
