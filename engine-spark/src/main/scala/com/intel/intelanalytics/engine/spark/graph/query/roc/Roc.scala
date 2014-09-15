package com.intel.intelanalytics.engine.spark.graph.query.roc

import scala.collection.mutable.ArrayBuffer

object RocCounts {

  /**
   * Calculate the number of true positives, false positives, positives, and negatives based on
   * posterior and prior probabilities.
   *
   * @param prior Prior probability
   * @param posterior Posterior probability
   * @param rocParams ROC parameters
   * @return Counts of true positives, false positives, positives, and negatives for each threshold
   */
  def updateRocCounts(prior: Double, posterior: Double, rocParams: RocParams): RocCounts = {

    val rocCounts = new RocCounts(rocParams.size)
    var k = 0
    for (k <- 0 until rocParams.size) {
      val rocThreshold = if (rocParams.thresholds(k) > rocParams.max) rocParams.max else rocParams.thresholds(k)
      if (prior >= rocThreshold) {
        rocCounts.numPositives(k) += 1
        if (posterior >= rocThreshold) {
          rocCounts.numTruePositives(k) += 1
        }
      }
      else {
        rocCounts.numNegatives(k) += 1
        if (posterior >= rocThreshold) {
          rocCounts.numFalsePositives(k) += 1
        }
      }
    }
    rocCounts
  }

  /**
   * Calculate true positive and false positive rates for each threshold
   *
   * @param rocCounts Counts of true positives, false positives, positives, and negatives
   * @param rocParams  ROC parameters
   * @return True positive and false positive rates for each threshold
   */
  def calcRoc(rocCounts: RocCounts, rocParams: RocParams): Seq[Roc] = {
    val rocList = ArrayBuffer[Roc]()
    var k = 0
    for (k <- 0 until rocParams.size) {
      val fpr = if (rocCounts.numNegatives(k) > 0) rocCounts.numFalsePositives(k) / rocCounts.numNegatives(k).toFloat else 0d
      val tpr = if (rocCounts.numPositives(k) > 0) rocCounts.numTruePositives(k) / rocCounts.numPositives(k).toFloat else 0d
      rocList += Roc(rocParams.thresholds(k), fpr, tpr)
    }
    rocList += Roc(0, 0, 0)
    rocList += Roc(1, 1, 1)
    rocList.sortBy(_.falsePositiveRate).toSeq
  }
}

/**
 * ROC threshold parameters comprising of minimum, maximum, and step for ROC curve
 *
 * @param args String with ROC threshold parameters in "min:step:max" format
 */
case class RocParams(args: List[Double]) {
  // TODO: list positions shouldn't have special meaning, this class should take three args, please fix me!
  require(args.size == 3, "Please input roc threshold in 'min:step:max' format")

  val min = args(0)
  val step = args(1)
  val max = args(2)
  val size = ((max - min) / step).toInt

  val thresholds = (0 until size).map(i => BigDecimal(min + step * i).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
}

/**
 * True positive rate, and false positive rate for ROC threshold.
 *
 * @param threshold ROC threshold
 * @param falsePositiveRate False positive rate
 * @param truePositiveRate True positive rate
 */
case class Roc(threshold: Double, falsePositiveRate: Double, truePositiveRate: Double)

/**
 * ROC curve.
 *
 * @param splitType  Split type
 * @param rocCurve  Sorted ROC values for a range of thresholds
 */
case class RocCurve(splitType: String, rocCurve: Seq[Roc])

/**
 * Tracks the number of true positives, false positives, positives, and negatives for each ROC threshold.
 *
 * @param numTruePositives Count of true positives for each ROC threshold
 * @param numFalsePositives Count of false positives for each ROC threshold
 * @param numPositives Count of positives for each ROC threshold
 * @param numNegatives Count of negatives for each ROC threshold
 */
case class RocCounts(numTruePositives: Array[Long],
                     numFalsePositives: Array[Long],
                     numPositives: Array[Long],
                     numNegatives: Array[Long]) {

  def this(sizeThresholds: Int) = {
    this(new Array[Long](sizeThresholds),
      new Array[Long](sizeThresholds),
      new Array[Long](sizeThresholds),
      new Array[Long](sizeThresholds))
  }

  /**
   * Returns sum of ROC counts.
   *
   * @param rocCountsToAdd ROC counts to add
   * @return Sum of ROC counts
   */
  def merge(rocCountsToAdd: RocCounts): RocCounts = {
    val numTruePositives = add(this.numTruePositives, rocCountsToAdd.numTruePositives)
    val numFalsePositives = add(this.numFalsePositives, rocCountsToAdd.numFalsePositives)
    val numPositives = add(this.numPositives, rocCountsToAdd.numPositives)
    val numNegatives = add(this.numNegatives, rocCountsToAdd.numNegatives)

    new RocCounts(numTruePositives, numFalsePositives, numPositives, numNegatives)
  }

  /**
   * Add two arrays.
   *
   * @param arr1 First array
   * @param arr2 Second array
   * @return Sum of first and second array
   */
  def add(arr1: Array[Long], arr2: Array[Long]): Array[Long] = {
    arr1.zip(arr2).map { case (a, b) => a + b }
  }

}

