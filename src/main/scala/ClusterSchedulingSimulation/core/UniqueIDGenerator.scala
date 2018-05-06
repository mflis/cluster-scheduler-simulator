package ClusterSchedulingSimulation.core

object UniqueIDGenerator {
  var counter = 0

  def getUniqueID(): Int = {
    counter += 1
    counter
  }
}
