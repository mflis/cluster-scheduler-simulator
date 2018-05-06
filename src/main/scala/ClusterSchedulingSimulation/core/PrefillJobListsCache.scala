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

object PrefillJobListsCache {
  // Map from (workloadname, traceFileName) -> list of jobs.
  val jobLists =
    collection.mutable.Map[(String, String), Iterable[Job]]()
  val cpusPerTaskDistributions =
    collection.mutable.Map[String, Array[Double]]()
  val memPerTaskDistributions =
    collection.mutable.Map[String, Array[Double]]()

  /**
    * When we load jobs from a trace file, we fill in the duration for all jobs
    * that don't have an end event as -1, then we fill in the duration for all
    * such jobs.
    */
  def getJobs(workloadName: String,
              traceFileName: String,
              timeWindow: Double): Iterable[Job] = {
    val jobs = getOrLoadJobs(workloadName, traceFileName)
    // Update duration of jobs with duration set to -1.
    jobs.foreach(job => {
      if (job.taskDuration == -1)
        job.taskDuration = timeWindow
    })
    jobs
  }

  def getCpusPerTaskDistribution(workloadName: String,
                                 traceFileName: String): Array[Double] = {
    //TODO(andyk): Fix this ugly hack of prepending "Prefill".
    val jobs = getOrLoadJobs("Prefill" + workloadName, traceFileName)
    println("Loaded or retreived %d %s jobs in tracefile %s."
      .format(jobs.size, "Prefill" + workloadName, traceFileName))
    cpusPerTaskDistributions.getOrElseUpdate(
      traceFileName, buildDist(jobs.map(_.cpusPerTask).toArray))
  }

  def getMemPerTaskDistribution(workloadName: String,
                                traceFileName: String): Array[Double] = {
    //TODO(andyk): Fix this ugly hack of prepending "Prefill".
    val jobs = getOrLoadJobs("Prefill" + workloadName, traceFileName)
    println("Loaded or retreived %d %s jobs in tracefile %s."
      .format(jobs.size, "Prefill" + workloadName, traceFileName))
    memPerTaskDistributions.getOrElseUpdate(
      traceFileName, buildDist(jobs.map(_.memPerTask).toArray))
  }

  def getOrLoadJobs(workloadName: String,
                    traceFileName: String): Iterable[Job] = {
    val cachedJobs = jobLists.getOrElseUpdate((workloadName, traceFileName), {
      val newJobs = collection.mutable.Map[String, Job]()
      val traceSrc = io.Source.fromFile(traceFileName)
      val lines: Iterator[String] = traceSrc.getLines()
      lines.foreach(line => {
        val parsedLine = line.split(" ")
        // The following parsing code is based on the space-delimited schema
        // used in textfile. See the README Andy made for a description of it.

        // Parse the fields that are common between both (6 & 8 column)
        // row formats.
        val timestamp: Double = parsedLine(1).toDouble
        val jobID: String = parsedLine(2)
        val isHighPriority: Boolean = if (parsedLine(3).equals("1")) {
          true
        }
        else {
          false
        }
        val schedulingClass: Int = parsedLine(4).toInt
        // Label the job according to PBB workload split. SchedulingClass 0 & 1
        // are batch, 2 & 3 are service.
        // (isServiceJob == false) => this is a batch job.
        val isServiceJob = (isHighPriority && schedulingClass != 0 && schedulingClass != 1)
        // Add the job to this workload if its job type is the same as
        // this generator's, which we determine based on workloadName
        // and if we havne't reached the resource size limits of the
        // requested workload.
        if ((isServiceJob && workloadName.equals("PrefillService")) ||
          (!isServiceJob && workloadName.equals("PrefillBatch")) ||
          (workloadName.equals("PrefillBatchService"))) {
          if (parsedLine(0).equals("11")) {
            assert(parsedLine.length == 8, "Found %d fields, expecting %d"
              .format(parsedLine.length, 8))
            val numTasks: Int = parsedLine(5).toInt
            val cpusPerJob: Double = parsedLine(6).toDouble
            // The tracefile has memory in bytes, our simulator is in GB.
            val memPerJob: Double = parsedLine(7).toDouble / 1024 / 1024 / 1024
            val newJob = Job(UniqueIDGenerator.getUniqueID(),
              0.0, // Submitted
              numTasks,
              -1, // to be replaced w/ simulator.runTime
              workloadName,
              cpusPerJob / numTasks,
              memPerJob / numTasks)
            newJobs(parsedLine(2)) = newJob
            // Update the job/task duration for jobs that we have end times for.
          } else if (parsedLine(0).equals("12")) {
            assert(parsedLine.length == 6, "Found %d fields, expecting %d"
              .format(parsedLine.length, 8))
            assert(newJobs.contains(jobID), "Expect to find job %s in newJobs."
              .format(jobID))
            newJobs(jobID).taskDuration = timestamp
          } else {
            sys.error("Invalid trace event type code %s in tracefile %s"
              .format(parsedLine(0), traceFileName))
          }
        }
      })
      traceSrc.close()
      // Add the newly parsed list of jobs to the cache.
      println("loaded %d newJobs.".format(newJobs.size))
      newJobs.values
    })
    // Return a copy of the cached jobs since the job durations will
    // be updated according to the experiment time window.
    println("returning %d jobs from cache".format(cachedJobs.size))
    cachedJobs.map(_.copy())
  }

  def buildDist(dataPoints: Array[Double]): Array[Double] = {
    val refDistribution = new Array[Double](1001)
    assert(dataPoints.length > 0,
      "dataPoints must contain at least one data point.")
    util.Sorting.quickSort(dataPoints)
    for (i <- 0 to 1000) {
      // Store summary quantiles. 99.9 %tile = length * .999
      val index = ((dataPoints.length - 1) * i / 1000.0).toInt
      val currPercentile =
        dataPoints(index)
      refDistribution(i) = currPercentile
    }
    refDistribution
  }
}
