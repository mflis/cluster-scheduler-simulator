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
