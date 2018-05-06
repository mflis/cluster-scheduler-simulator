package ClusterSchedulingSimulation.mesos

import ClusterSchedulingSimulation.core.SchedulerDesc

class MesosSchedulerDesc(name: String,
                         constantThinkTimes: Map[String, Double],
                         perTaskThinkTimes: Map[String, Double],
                         val schedulePartialJobs: Boolean)
  extends SchedulerDesc(name,
    constantThinkTimes,
    perTaskThinkTimes)
