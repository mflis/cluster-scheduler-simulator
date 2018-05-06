package ClusterSchedulingSimulation.core

/**
  * A simulator to compare different cluster scheduling architectures
  * (single agent, dynamically partitioned,and replicated state), based
  * on a set of input parameters that define the schedulers being used
  * and the workload being played.
  *
  * @param schedulers             A Map from schedulerName to Scheduler, should
  *                               exactly one entry for each scheduler that is registered with
  *                               this simulator.
  * @param workloadToSchedulerMap A map from workloadName to Seq[SchedulerName].
  *                               Used to determine which scheduler each job is assigned.
  */
class ClusterSimulator(val cellState: CellState,
                       val schedulers: Map[String, Scheduler],
                       val workloadToSchedulerMap: Map[String, Seq[String]],
                       val workloads: Seq[Workload],
                       prefillWorkloads: Seq[Workload],
                       logging: Boolean = false,
                       monitorUtilization: Boolean = true,
                       monitoringPeriod: Double = 1.0)
  extends Simulator(logging) {
  assert(schedulers.size > 0, "At least one scheduler must be provided to" +
    "scheduler constructor.")
  assert(workloadToSchedulerMap.size > 0, "No workload->scheduler map setup.")
  workloadToSchedulerMap.values.foreach(schedulerNameSeq => {
    schedulerNameSeq.foreach(schedulerName => {
      val contains: Boolean = schedulers.contains(schedulerName)
      assert(contains, ("Workload-Scheduler map points to a scheduler, " +
        "%s, that is not registered").format(schedulerName))
    })
  })
  // Set up a pointer to this simulator in the cellstate.
  cellState.simulator = this
  // Set up a pointer to this simulator in each scheduler.
  schedulers.values.foreach(_.simulator = this)

  private val prefillScheduler = new PrefillScheduler
  var roundRobinCounter = 0
  prefillScheduler.simulator = this
  // Prefill jobs that exist at the beginning of the simulation.
  // Setting these up is similar to loading jobs that are part
  // of the simulation run; they need to be scheduled onto machines
  println("Prefilling cell-state with %d workloads."
    .format(prefillWorkloads.length))
  prefillWorkloads.foreach(workload => {
    println("Prefilling cell-state with %d jobs from workload %s."
      .format(workload.numJobs, workload.name))
    var i = 0
    workload.getJobs.foreach(job => {
      i += 1
      // println("Prefilling %d %s job id - %d."
      //         .format(i, workload.name, job.id))
      if (job.cpusPerTask > cellState.cpusPerMachine ||
        job.memPerTask > cellState.memPerMachine) {
        println(("IGNORING A JOB REQUIRING %f CPU & %f MEM PER TASK " +
          "BECAUSE machines only have %f cpu / %f mem.")
          .format(job.cpusPerTask, job.memPerTask,
            cellState.cpusPerMachine, cellState.memPerMachine))
      } else {
        val claimDeltas = prefillScheduler.scheduleJob(job, cellState)
        // assert(job.numTasks == claimDeltas.length,
        //        "Prefill job failed to schedule.")
        cellState.scheduleEndEvents(claimDeltas)

        log(("After prefill, common cell state now has %.2f%% (%.2f) " +
          "cpus and %.2f%% (%.2f) mem occupied.")
          .format(cellState.totalOccupiedCpus / cellState.totalCpus * 100.0,
            cellState.totalOccupiedCpus,
            cellState.totalOccupiedMem / cellState.totalMem * 100.0,
            cellState.totalOccupiedMem))
      }
    })
  })

  // Set up workloads
  workloads.foreach(workload => {
    var numSkipped, numLoaded = 0
    workload.getJobs.foreach(job => {
      val scheduler = getSchedulerForWorkloadName(job.workloadName)
      if (scheduler == None) {
        log(("Warning, skipping a job fom a workload type (%s) that has " +
          "not been mapped to any registered schedulers. Please update " +
          "a mapping for this scheduler via the workloadSchedulerMap param.")
          .format(job.workloadName))
        numSkipped += 1
      } else {
        // Schedule the task to get submitted to its scheduler at its
        // submission time.

        // assert(job.cpusPerTask * job.numTasks <= cellState.totalCpus + 0.000001 &&
        //        job.memPerTask * job.numTasks <= cellState.totalMem + 0.000001,
        //        ("The cell (%f cpus, %f mem) is not big enough to hold job %d " +
        //        "all at once which requires %f cpus and %f mem in total.")
        //        .format(cellState.totalCpus,
        //                cellState.totalMem,
        //                job.id,
        //                job.cpusPerTask * job.numTasks,
        //                job.memPerTask * job.numTasks))
        if (job.cpusPerTask * job.numTasks > cellState.totalCpus + 0.000001 ||
          job.memPerTask * job.numTasks > cellState.totalMem + 0.000001) {
          println(("WARNING: The cell (%f cpus, %f mem) is not big enough " +
            "to hold job id %d all at once which requires %f cpus " +
            "and %f mem in total.").format(cellState.totalCpus,
            cellState.totalMem,
            job.id,
            job.cpusPerTask * job.numTasks,
            job.memPerTask * job.numTasks))
        }
        afterDelay(job.submitted - currentTime) {
          scheduler.foreach(_.addJob(job))
        }
        numLoaded += 1
      }
    })
    println("Loaded %d jobs from workload %s, and skipped %d.".format(
      numLoaded, workload.name, numSkipped))
  })
  var sumCpuUtilization: Double = 0.0
  var sumMemUtilization: Double = 0.0
  var sumCpuLocked: Double = 0.0
  var sumMemLocked: Double = 0.0
  var numMonitoringMeasurements: Long = 0

  // If more than one scheduler is assigned a workload, round robin across them.
  def getSchedulerForWorkloadName(workloadName: String): Option[Scheduler] = {
    workloadToSchedulerMap.get(workloadName).map(schedulerNames => {
      // println("schedulerNames is %s".format(schedulerNames.mkString(" ")))
      roundRobinCounter += 1
      val name = schedulerNames(roundRobinCounter % schedulerNames.length)
      // println("Assigning job from workload %s to scheduler %s"
      //         .format(workloadName, name))
      schedulers(name)
    })
  }

  // Track utilization due to resources actually being accepted by a
  // framework/scheduler. This does not include the time resources spend
  // tied up while they are pessimistically locked (e.g. while they are
  // offered as part of a Mesos resource-offer). That type of utilization
  // is tracked separately below.
  def avgCpuUtilization: Double = sumCpuUtilization / numMonitoringMeasurements

  def avgMemUtilization: Double = sumMemUtilization / numMonitoringMeasurements

  // Track "utilization" of resources due to their being pessimistically locked
  // (i.e. while they are offered as part of a Mesos resource-offer).
  def avgCpuLocked: Double = sumCpuLocked / numMonitoringMeasurements

  def avgMemLocked: Double = sumMemLocked / numMonitoringMeasurements

  def measureUtilization: Unit = {
    numMonitoringMeasurements += 1
    sumCpuUtilization += cellState.totalOccupiedCpus
    sumMemUtilization += cellState.totalOccupiedMem
    sumCpuLocked += cellState.totalLockedCpus
    sumMemLocked += cellState.totalLockedMem
    log("Avg cpu utilization (adding measurement %d of %f): %f."
      .format(numMonitoringMeasurements,
        cellState.totalOccupiedCpus,
        avgCpuUtilization))
    //Temporary: print utilization throughout the day.
    // if (numMonitoringMeasurements % 1000 == 0) {
    //   println("%f - Current cluster utilization: %f %f cpu , %f %f mem"
    //           .format(currentTime,
    //                   cellState.totalOccupiedCpus,
    //                   cellState.totalOccupiedCpus / cellState.totalCpus,
    //                   cellState.totalOccupiedMem,
    //                   cellState.totalOccupiedMem / cellState.totalMem))
    //   println(("%f - Current cluster utilization from locked resources: " +
    //            "%f cpu, %f mem")
    //            .format(currentTime,
    //                    cellState.totalLockedCpus,
    //                    cellState.totalLockedCpus/ cellState.totalCpus,
    //                    cellState.totalLockedMem,
    //                    cellState.totalLockedMem / cellState.totalMem))
    // }
    log("Avg mem utilization: %f.".format(avgMemUtilization))
    // Only schedule a monitoring event if the simulator has
    // more (non-monitoring) events to play. Else this will cause
    // the simulator to run forever just to keep monitoring.
    if (!agenda.isEmpty) {
      afterDelay(monitoringPeriod) {
        measureUtilization
      }
    }
  }

  override
  def run(runTime: Option[Double] = None,
          wallClockTimeout: Option[Double] = None): Boolean = {
    assert(currentTime == 0.0, "currentTime must be 0 at simulator run time.")
    schedulers.values.foreach(scheduler => {
      assert(scheduler.jobQueueSize == 0,
        "Schedulers are not allowed to have jobs in their " +
          "queues when we run the simulator.")
    })
    // Optionally, start the utilization monitoring loop.
    if (monitorUtilization) {
      afterDelay(0.0) {
        measureUtilization
      }
    }
    super.run(runTime, wallClockTimeout)
  }

  class PrefillScheduler
    extends Scheduler(name = "prefillScheduler",
      constantThinkTimes = Map[String, Double](),
      perTaskThinkTimes = Map[String, Double](),
      numMachinesToBlackList = 0) {}

}
