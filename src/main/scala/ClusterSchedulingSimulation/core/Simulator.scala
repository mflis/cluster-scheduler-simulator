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
  * A simple, generic, discrete event simulator. A modified version of the
  * basic discrete event simulator code from Programming In Scala, pg 398,
  * http://www.amazon.com/Programming-Scala-Comprehensive-Step-step/dp/0981531601
  */
abstract class Simulator(logging: Boolean = false) {
  type Action = () => Unit
  protected var curtime: Double = 0.0 // simulation time, in seconds

  implicit object WorkItemOrdering extends Ordering[WorkItem] {
    def compare(a: WorkItem, b: WorkItem) = {
      if (a.time < b.time) 1
      else if (a.time > b.time) -1
      else 0
    }
  }

  protected var agenda = new collection.mutable.PriorityQueue[WorkItem]()

  def agendaSize = agenda.size

  def log(s: => String) {
    if (logging) {
      println(curtime + " " + s)
    }
  }

  /**
    * Run the simulation for {@code runTime} virtual (i.e., simulated)
    * seconds or until {@code wallClockTimeout} seconds of execution
    * time elapses.
    *
    * @return true if simulation ran till runTime or completion, and false
    *         if simulation timed out.
    */
  def run(runTime: Option[Double] = None,
          wallClockTimeout: Option[Double] = None): Boolean = {
    afterDelay(0) {
      println("*** Simulation started, time = " + currentTime + ". ***")
    }
    // Record wall clock time at beginning of simulation run.
    val startWallTime = java.util.Calendar.getInstance().getTimeInMillis()

    def timedOut: Boolean = {
      val currWallTime = java.util.Calendar.getInstance().getTimeInMillis()
      if (wallClockTimeout.exists((currWallTime - startWallTime) / 1000.0 > _)) {
        println("Execution timed out after %f seconds, ending simulation now."
          .format((currWallTime - startWallTime) / 1000.0))
        true
      } else {
        false
      }
    }

    runTime match {
      case Some(rt) =>
        while (!agenda.isEmpty && currentTime <= rt && !timedOut) next()
      case None =>
        while (!agenda.isEmpty && !timedOut) next()
    }
    println("*** Simulation finished running, time = " + currentTime + ". ***")
    !timedOut
  }

  def afterDelay(delay: Double)(block: => Unit) {
    val item = WorkItem(currentTime + delay, () => block)
    agenda.enqueue(item)
    // log("inserted new WorkItem into agenda to run at time " + (currentTime + delay))
  }

  def currentTime: Double = curtime

  private def next() {
    val item = agenda.dequeue()
    curtime = item.time
    item.action()
  }

  case class WorkItem(time: Double, action: Action)

}
