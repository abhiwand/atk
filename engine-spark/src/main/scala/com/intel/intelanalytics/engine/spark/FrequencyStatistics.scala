package com.intel.intelanalytics.engine.spark

import org.apache.spark.{ AccumulatorParam, Accumulator, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

case class FrequencyStatsCounter[T](mode: T, weightOfMode: Double, totalWeight: Double) extends Serializable

class frequencyStatsAccumulatorParam[T](nonValue: T) extends AccumulatorParam[FrequencyStatsCounter[T]] with Serializable {

  override def zero(initialValue: FrequencyStatsCounter[T]) = FrequencyStatsCounter[T](nonValue, -1, 0)

  override def addInPlace(r1: FrequencyStatsCounter[T], r2: FrequencyStatsCounter[T]): FrequencyStatsCounter[T] = {
    if (r1.weightOfMode > r2.weightOfMode) {
      FrequencyStatsCounter[T](r1.mode, r1.weightOfMode, r1.totalWeight + r2.totalWeight)
    }
    else {
      FrequencyStatsCounter[T](r2.mode, r2.weightOfMode, r1.totalWeight + r2.totalWeight)
    }
  }
}

class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)], nonValue: T) extends Serializable {

  lazy val modeAndNetWeight: (T, Double) = generateMode()

  private def generateMode(): (T, Double) = {

    val acumulatorParam = new frequencyStatsAccumulatorParam[T](nonValue)
    val initialValue = FrequencyStatsCounter[T](nonValue, -1, 0)
    val accumulator =
      dataWeightPairs.sparkContext.accumulator[FrequencyStatsCounter[T]](initialValue)(acumulatorParam)

    val weightsGroupedByValues: RDD[(T, Seq[(T,Double)])] = dataWeightPairs.groupBy(_._1)

    val valueNetWeightPairs: RDD[(T, Double)] = weightsGroupedByValues
      .map({ case (value, weightList) => (value, weightList.map(_._2).reduce(_ + _)) })

    valueNetWeightPairs.foreach({
      case (value, weightAtValue) => accumulator.add(FrequencyStatsCounter[T](value, weightAtValue, weightAtValue))
    })

    val mode: T = accumulator.value.mode
    val weightOfMode: Double = accumulator.value.weightOfMode
    val totalWeight: Double = accumulator.value.totalWeight

    (mode, weightOfMode / totalWeight)
  }
}