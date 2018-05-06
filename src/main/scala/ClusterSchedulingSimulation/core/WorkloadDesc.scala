package ClusterSchedulingSimulation.core

case class WorkloadDesc(cell: String,
                        assignmentPolicy: String,
                        // getJob(0) is called
                        workloadGenerators: List[WorkloadGenerator],
                        cellStateDesc: CellStateDesc,
                        prefillWorkloadGenerators: List[WorkloadGenerator] =
                        List[WorkloadGenerator]()) {
  assert(!cell.contains(" "), "Cell names cannot have spaces in them.")
  assert(!assignmentPolicy.contains(" "),
    "Assignment policies cannot have spaces in them.")
  assert(prefillWorkloadGenerators.length ==
    prefillWorkloadGenerators.map(_.workloadName).toSet.size)
}
