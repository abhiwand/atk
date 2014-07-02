package com.intel.intelanalytics.engine.spark.graph.query.roc

case class Roc(threshold: Float, fpr: Float, tpr: Float) extends Serializable

case class RocParams(rocThreshold: List[Double]) extends Serializable {
  require(rocThreshold.size == 3, "Please input roc threshold in 'min:step:max' format")

  // TODO: Type check
  val rocMin = rocThreshold(0)
  val rocStep = rocThreshold(1)
  val rocMax = rocThreshold(2)
  val rocSize = ((rocMax - rocMin) / rocStep).toInt
}

case class RocCounts(numTruePositives: Array[Int],
                     numFalsePositives: Array[Int],
                     numPositives: Array[Int],
                     numNegatives: Array[Int]) extends Serializable {

  def this(numBins: Int) = {
    this(new Array[Int](numBins),
      new Array[Int](numBins),
      new Array[Int](numBins),
      new Array[Int](numBins))
  }

  def add(rocCounts: RocCounts): RocCounts = {
    val newTpCount = this.numTruePositives.zip(rocCounts.numTruePositives).map { case (a, b) => a + b }
    val newFpCount = this.numFalsePositives.zip(rocCounts.numFalsePositives).map { case (a, b) => a + b }
    val newPosCount = this.numPositives.zip(rocCounts.numPositives).map { case (a, b) => a + b }
    val newNegCount = this.numNegatives.zip(rocCounts.numNegatives).map { case (a, b) => a + b }

    new RocCounts(newTpCount, newFpCount, newPosCount, newNegCount)
  }

  def prettyPrint() {
    println(this.numTruePositives.deep.mkString(","))
  }

}

