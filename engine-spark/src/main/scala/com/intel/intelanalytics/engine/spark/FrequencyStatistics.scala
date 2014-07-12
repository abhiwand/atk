package com.intel.intelanalytics.engine.spark

import org.apache.spark.{ AccumulatorParam, Accumulator, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class FrequencyStatsCounter[T](mode: T, weightOfMode: Double, totalWeight: Double) extends Serializable

class frequencyStatsAccumulatorParam[T](nonValue: T) extends AccumulatorParam[FrequencyStatsCounter] with Serializable {

  override def zero(initialValue: FrequencyStatsCounter) = FrequencyStatsCounter(nonValue, -1, 0)

  override def addInPlace(r1: FrequencyStatsCounter, r2: FrequencyStatsCounter): FrequencyStatsCounter = {
    if (r1.weightOfMode > r2.weightOfMode) {
      FrequencyStatsCounter(r1.mode, r1.weightOfMode, r1.totalWeight + r2.totalWeight)
    }
    else {
      FrequencyStatsCounter(r2.mode, r2.weightOfMode, r1.totalWeight + r2.totalWeight)
    }
  }
}

class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)], nonValue: T) extends Serializable {

  lazy val modeAndNetWeight: (T, Double) = generateMode()

  private def generateMode(): (T, Double) = {

    val acumulatorParam = new frequencyStatsAccumulatorParam[T](nonValue)
    val initialValue = FrequencyStatsCounter(nonValue, -1, 0)
    val accumulator =
      dataWeightPairs.sparkContext.accumulator[FrequencyStatsCounter](initialValue)(acumulatorParam)

    val groupedByValues = dataWeightPairs.groupBy((x: (T, Double)) => x._1)
    val valueNetWeightPairs = groupedByValues
      .map({ case (value: T, weightList: Seq[(T, Double)]) => (value, weightList.map(_._2).reduce(_ + _)) })

    valueNetWeightPairs.foreach({
      case (value, weightAtValue) => accumulator.add(FrequencyStatsCounter(value, weightAtValue, weightAtValue))
    })

    val mode: T = accumulator.value.mode
    val weightOfMode: Double = accumulator.value.weightOfMode
    val totalWeight: Double = accumulator.value.totalWeight

    println("The mode is " + mode)
    println("The weight of the mode is " + weightOfMode)
    println("The total weight is " + totalWeight)
    (mode, totalWeight)
  }
}