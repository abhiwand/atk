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
 * @tparam T Value type.
 */
class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)]) extends Serializable {

  /**
   * Set of at most k modes. A mode is an item with maximum weight. If there is no item with positive weight,
   * the returned set is empty.
   */
  lazy val modeSet: Set[T] = frequencyStatistics.mode

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

  private lazy val frequencyStatistics: FrequencyStatsCounter[T] = generateMode()

  private def generateMode(): FrequencyStatsCounter[T] = {

    val acumulatorParam = new FrequencyStatsAccumulatorParam[T]()
    val initialValue = FrequencyStatsCounter[T](Set.empty[T], 0, 0, 0)

    val accumulator =
      dataWeightPairs.sparkContext.accumulator[FrequencyStatsCounter[T]](initialValue)(acumulatorParam)

    val dataWeightPairsPositiveWeights =
      dataWeightPairs.filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val uniqueValuesPositiveWeights: RDD[(T, Double)] =
      dataWeightPairsPositiveWeights.groupBy(_._1).map({ case (data, weights) => aggregateWeights(data, weights) })

    uniqueValuesPositiveWeights.foreach(
      {
        case (value, weightAtValue) =>
          accumulator.add(FrequencyStatsCounter(Set(value), weightAtValue, weightAtValue, 1))
      })

    FrequencyStatsCounter[T](accumulator.value.modeSet,
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
private case class FrequencyStatsCounter[T](modeSet: Set[T], weightOfMode: Double, totalWeight: Double, modeCount: Long)
  extends Serializable

/*
 * Configures the spark accumulator for gathering frequency statistics.
 * @tparam T The type of the input data.
 */
private class FrequencyStatsAccumulatorParam[T] extends AccumulatorParam[FrequencyStatsCounter[T]] with Serializable {

  override def zero(initialValue: FrequencyStatsCounter[T]) = FrequencyStatsCounter(Set.empty[T], 0, 0, 0)

  // to get (more) reproducible results, in the case that two modes have the same weight, we opt for the mode with the
  // lesser hashcode...
  // of course, this is not perfect since there can and will be collisions in the 32 bit hashes
  // TODO: investigate requiring that the type parameter T for the FrequencyStats class implement the Ordered trait
  // not sure if we want to add that over head... it may be that we simply opt for a slicker way of handling
  // vastly multimodal data

  override def addInPlace(stats1: FrequencyStatsCounter[T], stats2: FrequencyStatsCounter[T]): FrequencyStatsCounter[T] = {
    if (stats1.mode.isEmpty) {
      stats2
    }
    else if (stats2.mode.isEmpty) {
      stats1
    }
    else {
      if (stats1.weightOfMode > stats2.weightOfMode) {
        FrequencyStatsCounter(stats1.mode, stats1.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats1.modeCount)
      }
      else if (stats1.weightOfMode < stats2.weightOfMode) {
        FrequencyStatsCounter(stats2.mode,
          stats2.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats2.modeCount)
      }
      else {
        val kLeastModes = getFirstKOfTwoKSets()
        FrequencyStatsCounter(stats1.mode,
          stats1.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats1.modeCount + stats2.modeCount)
      }
    }
  }

  def getFirstKOfTwoKSets[K, V: Ordering](inputIterator: Iterator[(K, V)], k: Int): Iterator[(K, V)] = {

    val ordering = implicitly[Ordering[V]]

    var treeMap = new TreeMap[V, K]()(ordering)

    inputIterator.foreach({
      case (key, value) =>
        treeMap += (value -> key)
        if (treeMap.size > k) treeMap = treeMap.dropRight(1)
    })

    val sortedK = for ((value, key) <- treeMap.toSeq) yield (key, value)
    sortedK.toIterator
  }

}
