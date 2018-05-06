package ClusterSchedulingSimulation.core

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
