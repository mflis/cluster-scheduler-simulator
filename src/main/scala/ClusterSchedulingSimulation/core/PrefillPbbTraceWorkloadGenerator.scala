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

package ClusterSchedulingSimulation.core


/**
  * Generates a pre-fill workload based on an input trace from a cluster.
  * Given a workloadName, it has hard coded rules to split up jobs
  * in the input file according to the PBB style split based on
  * the jobs' priority level and schedulingClass.
  */
class PrefillPbbTraceWorkloadGenerator(val workloadName: String,
                                       traceFileName: String)
  extends WorkloadGenerator {
  assert(workloadName.equals("PrefillBatch") ||
    workloadName.equals("PrefillService") ||
    workloadName.equals("PrefillBatchService"))

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime == None)
    assert(timeWindow >= 0)

    var joblist = PrefillJobListsCache.getJobs(workloadName,
      traceFileName,
      timeWindow)
    val workload = new Workload(workloadName)

    //TODO(andyk): Make this more functional.
    def reachedMaxCpu(currCpus: Double) = maxCpus.exists(currCpus >= _)

    def reachedMaxMem(currMem: Double) = maxMem.exists(currMem >= _)

    var iter = joblist.toIterator
    var numCpus = 0.0
    var numMem = 0.0
    var counter = 0
    while (iter.hasNext) {
      counter += 1
      val nextJob = iter.next
      numCpus += nextJob.numTasks * nextJob.cpusPerTask
      numMem += nextJob.numTasks * nextJob.memPerTask
      if (reachedMaxCpu(numCpus) || reachedMaxMem(numMem)) {
        println("reachedMaxCpu = %s, reachedMaxMem = %s"
          .format(reachedMaxCpu(numCpus), reachedMaxMem(numMem)))
        iter = Iterator()
      } else {
        // Use copy to be sure we don't share state between runs of the
        // simulator.
        workload.addJob(nextJob.copy())
        iter = iter.drop(0)
      }
    }
    println("Returning workload with %f cpus and %f mem in total."
      .format(workload.getJobs.map(j => {
        j.numTasks * j.cpusPerTask
      }).sum,
        workload.getJobs.map(j => {
          j.numTasks * j.memPerTask
        }).sum))
    workload
  }
}

