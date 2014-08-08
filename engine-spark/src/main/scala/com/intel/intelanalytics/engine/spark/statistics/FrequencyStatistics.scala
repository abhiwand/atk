package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd._
import scala.collection.immutable.TreeMap

/**
 * Object for calculating the frequency statistics of a collection of (data,weight) pairs, represented as an
 * RDD of (T,Double) pairs, where T is a type parameter.
 *
 * All data items with weights <= 0 are excluded from these calculations.
 *
 * @param dataWeightPairs RDD containing pairs (data, weight) where the each "data" entry is unique.
 * @param k Maximum number of items returned.
 * @tparam T Value type.
 */
class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)], k: Int)
    extends Serializable {

  /**
   * Set of at most k modes. A mode is an item with maximum weight. If there is no item with positive weight,
   * the returned set is empty.
   */
  lazy val modeSet: Set[T] = frequencyStatistics.modes.toSet[T]

  /**
   * The weight of a mode of the input. It is either strictly positive, or,
   * if there is no item with positive weight, weightOfMode is 0 .
   */
  lazy val weightOfMode: Double = frequencyStatistics.weightOfMode

  /**
   * The number of modes in the data.
   */
  lazy val modeCount: Long = frequencyStatistics.modeCount

  /**
   * Sum of all weights.
   */
  lazy val totalWeight: Double = frequencyStatistics.totalWeight

  private lazy val frequencyStatistics: FrequencyStatsCounter[T] = generateMode(k)

  private def generateMode(k: Int): FrequencyStatsCounter[T] = {

    val acumulatorParam = new FrequencyStatsAccumulatorParam[T](k)
    val initialValue = FrequencyStatsCounter[T](List.empty[T], 0, 0, 0)

    val accumulator =
      dataWeightPairs.sparkContext.accumulator[FrequencyStatsCounter[T]](initialValue)(acumulatorParam)

    val dataWeightPairsPositiveWeights =
      dataWeightPairs.filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val uniqueValuesPositiveWeights: RDD[(T, Double)] =
      dataWeightPairsPositiveWeights.groupBy(_._1).map({ case (data, weights) => aggregateWeights(data, weights) })

    uniqueValuesPositiveWeights.foreach(
      {
        case (value, weightAtValue) =>
          accumulator.add(FrequencyStatsCounter(List(value), weightAtValue, weightAtValue, 1))
      })

    FrequencyStatsCounter[T](accumulator.value.modes,
      accumulator.value.weightOfMode,
      accumulator.value.totalWeight,
      accumulator.value.modeCount)

  }

  private def aggregateWeights(data: T, dataWeightPairs: Seq[(T, Double)]): (T, Double) = {
    (data, dataWeightPairs.map({ case (data, weight) => weight }).reduce(_ + _))
  }

}

/*
 * Class for accumulating frequency statistics in one pass over the data.
 * @param mode Set of <= k modes seen so far.
 * @param weightOfMode The weight of the mode. 0 when run over empty data.
 * @param totalWeight Sum of the weights of all values seen so far.
 * @tparam T Type of the input data. (In particular, the type of the mode.)
 */
private case class FrequencyStatsCounter[T](modes: List[T], weightOfMode: Double, totalWeight: Double, modeCount: Long)
  extends Serializable

/*
 * Configures the spark accumulator for gathering frequency statistics.
 * @tparam T The type of the input data.
 */
private class FrequencyStatsAccumulatorParam[T](k: Int) extends AccumulatorParam[FrequencyStatsCounter[T]] with Serializable {

  private val ordering = new canonicalOrdering[T]

  override def zero(initialValue: FrequencyStatsCounter[T]) = FrequencyStatsCounter(List.empty[T], 0, 0, 0)

  // to get (more) reproducible results, in the case that two modes have the same weight, we opt for the mode with the
  // lesser hashcode...
  // of course, this is not perfect since there can and will be collisions in the 32 bit hashes
  // TODO: investigate requiring that the type parameter T for the FrequencyStats class implement the Ordered trait
  // not sure if we want to add that over head... it may be that we simply opt for a slicker way of handling
  // vastly multimodal data

  override def addInPlace(stats1: FrequencyStatsCounter[T], stats2: FrequencyStatsCounter[T]): FrequencyStatsCounter[T] = {
    if (stats1.modes.isEmpty) {
      stats2
    }
    else if (stats2.modes.isEmpty) {
      stats1
    }
    else {
      if (stats1.weightOfMode > stats2.weightOfMode) {
        FrequencyStatsCounter(stats1.modes, stats1.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats1.modeCount)
      }
      else if (stats1.weightOfMode < stats2.weightOfMode) {
        FrequencyStatsCounter(stats2.modes,
          stats2.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats2.modeCount)
      }
      else {
        val kLeastModes = merge(stats1.modes, stats2.modes, k)(ordering)
        FrequencyStatsCounter(kLeastModes,
          stats1.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats1.modeCount + stats2.modeCount)
      }
    }
  }

  private def merge(list1: List[T], list2: List[T], k: Int)(implicit order: Ordering[T]): List[T] = {
    if (k <= 0) {
      List.empty[T]
    }
    else if (list1.isEmpty) {
      list2.take(k)
    }
    else if (list2.isEmpty) {
      list1.take(k)
    }
    else if (order.lt(list1.head, list2.head)) {
      list1.head :: merge(list1.drop(1), list2, k - 1)
    }
    else {
      list2.head :: merge(list1, list2.drop(1), k - 1)
    }
  }

  private class canonicalOrdering[T] extends Ordering[T] {
    def compare(a: T, b: T) = {
      if (a.isInstanceOf[Int]) {
        a.asInstanceOf[Int].compareTo(b.asInstanceOf[Int])
      }
      else if (a.isInstanceOf[Long]) {
        a.asInstanceOf[Long].compareTo(b.asInstanceOf[Long])
      }
      else if (a.isInstanceOf[Float]) {
        a.asInstanceOf[Float].compareTo(b.asInstanceOf[Float])
      }
      else if (a.isInstanceOf[Double]) {
        a.asInstanceOf[Double].compareTo(b.asInstanceOf[Double])
      }
      else if (a.isInstanceOf[String]) {
        a.asInstanceOf[String].compareTo(b.asInstanceOf[String])
      }
      else {
        throw new IllegalArgumentException("Attempt to get frequency statistics for unsupported datatypes.")
      }
    }
  }
}
