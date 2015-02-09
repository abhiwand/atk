package com.intel.intelanalytics.domain.graph

/**
 * parameters needed to assign_sample to a titan graph
 * @param graph graph to be sampled
 * @param samplePercentages percentages to use
 * @param sampleLabels labels to use if omitted will use Sample# unless there are 3 groups then TR, TE, VA
 * @param outputProperty property name for label
 * @param randomSeed random seed value to randomize the split
 */
case class AssignSampleTitanArgs(graph: GraphReference,
                                 samplePercentages: List[Double],
                                 sampleLabels: Option[List[String]] = None,
                                 outputProperty: Option[String] = None,
                                 randomSeed: Option[Int] = None) {
  require(graph != null, "AssignSample requires a non-null graph.")

  require(samplePercentages != null, "AssignSample requires that the percentages vector be non-null.")

  require(samplePercentages.forall(_ >= 0), "AssignSample requires that all percentages be non-negative")
  require(samplePercentages.reduce(_ + _) <= 1, "AssignSample requires that percentages sum to no more than 1")

  require(sampleLabels.isEmpty || sampleLabels.getOrElse(List()).size == samplePercentages.size,
    "AssignSample requires a label for each group")

  def getSampleLabels: List[String] = sampleLabels match {
    case Some(labels) => labels
    case None => {
      if (samplePercentages.length == 3) {
        List("TR", "TE", "VA")
      }
      else {
        (0 to samplePercentages.length - 1).map(i => s"Sample_$i").toList
      }
    }
  }

  def getOutputProperty: String = outputProperty.getOrElse("sample_bin")

  def getRandomSeed: Int = randomSeed.getOrElse(0)
}