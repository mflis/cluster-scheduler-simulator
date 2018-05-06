package ClusterSchedulingSimulation.omega

import ClusterSchedulingSimulation.core.SchedulerDesc

/**
  * A simple subclass of SchedulerDesc for extensibility to
  * for symmetry in the naming of the type so that we don't
  * have to use a SchedulerDesc for an OmegaSimulator.
  */
class OmegaSchedulerDesc(name: String,
                         constantThinkTimes: Map[String, Double],
                         perTaskThinkTimes: Map[String, Double])
  extends SchedulerDesc(name,
    constantThinkTimes,
    perTaskThinkTimes)
