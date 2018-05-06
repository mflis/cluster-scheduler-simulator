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
  * An object for building and caching emperical distributions
  * from traces. Caches them because they are expensive to build
  * (require reading from disk) and are re-used between different
  * experiments.
  */
object DistCache {
  val distributions =
    collection.mutable.Map[(String, String), Array[Double]]()

  def getDistribution(workloadName: String,
                      traceFileName: String): Array[Double] = {
    distributions.getOrElseUpdate((workloadName, traceFileName),
      buildDist(workloadName, traceFileName))
  }

  def buildDist(workloadName: String, traceFileName: String): Array[Double] = {
    assert(workloadName.equals("Batch") || workloadName.equals("Service"))
    val dataPoints = new collection.mutable.ListBuffer[Double]()
    val refDistribution = new Array[Double](1001)

    println(("Reading tracefile %s and building distribution of " +
      "job data points based on it.").format(traceFileName))
    val traceSrc = io.Source.fromFile(traceFileName)
    val lines = traceSrc.getLines()
    var realDataSum: Double = 0.0
    lines.foreach(line => {
      val parsedLine = line.split(" ")
      // The following parsing code is based on the space-delimited schema
      // used in textfile. See the README for a description.
      // val cell: Double = parsedLine(1).toDouble
      // val allocationPolicy: String = parsedLine(2)
      val isServiceJob: Boolean = if (parsedLine(2).equals("1")) {
        true
      }
      else {
        false
      }
      val dataPoint: Double = parsedLine(3).toDouble
      // Add the job to this workload if its job type is the same as
      // this generator's, which we determine based on workloadName.
      if ((isServiceJob && workloadName.equals("Service")) ||
        (!isServiceJob && workloadName.equals("Batch"))) {
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
    })
    assert(dataPoints.length > 0,
      "Trace file must contain at least one data point.")
    println(("Done reading tracefile of %d jobs, average of real data " +
      "points was %f. Now constructing distribution.")
      .format(dataPoints.length,
        realDataSum / dataPoints.length))
    val dataPointsArray = dataPoints.toArray
    util.Sorting.quickSort(dataPointsArray)
    for (i <- 0 to 1000) {
      // Store summary quantiles.
      // 99.9 %tile = length * .999
      val index = ((dataPointsArray.length - 1) * i / 1000.0).toInt
      val currPercentile =
        dataPointsArray(index)
      refDistribution(i) = currPercentile
      // println("refDistribution(%d) = dataPointsArray(%d) = %f"
      //         .format(i, index, currPercentile))
    }
    refDistribution
  }
}
