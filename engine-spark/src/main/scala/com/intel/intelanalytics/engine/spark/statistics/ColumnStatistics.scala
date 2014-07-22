package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.frame.{ ColumnMedianReturn, ColumnModeReturn, ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }
import com.intel.intelanalytics.engine.spark.statistics.numericalstatistics._
import spray.json._
import DefaultJsonProtocol._
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes

private[spark] object ColumnStatistics extends Serializable {

  /**
   * Calculate (weighted) mode of a data column, the weight of the mode, and the total weight of the column.
   * A mode is a value that has maximum weight. Values with non-positive weights are thrown out before the calculation
   * is performed.
   *
   * When the total weight is 0, the option None is given for the mode and the weight of the mode.
   *
   * @param dataColumnIndex Index of the column providing data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing weights. Must be numerical data.
   * @param rowRDD RDD of input rows
   * @return The mode of the column (as a string), the weight of the mode, and the total weight of the data.
   */
  def columnMode(dataColumnIndex: Int,
                 dataType: DataType,
                 weightsColumnIndexOption: Option[Int],
                 rowRDD: RDD[Row]): ColumnModeReturn = {

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, rowRDD)
    val frequencyStatistics = new FrequencyStatistics(dataWeightPairs)

    val modeJsValue: JsValue = if (frequencyStatistics.mode.isEmpty) {
      None.asInstanceOf[Option[String]].toJson
    }
    else {
      if (dataType == DataTypes.string) {
        frequencyStatistics.mode.get.asInstanceOf[String].toJson
      }
      else if (dataType == DataTypes.int32) {
        frequencyStatistics.mode.get.asInstanceOf[Int].toJson
      }
      else if (dataType == DataTypes.int64) {
        frequencyStatistics.mode.get.asInstanceOf[Long].toJson
      }
      else if (dataType == DataTypes.float32) {
        frequencyStatistics.mode.get.asInstanceOf[Float].toJson
      }
      else if (dataType == DataTypes.float64) {
        frequencyStatistics.mode.get.asInstanceOf[Double].toJson
      }
      else {
        throw new IllegalArgumentException("Mode calculation cannot handle datatype " + dataType.toString)
      }
    }

    ColumnModeReturn(modeJsValue, frequencyStatistics.weightOfMode, frequencyStatistics.totalWeight)

  }

  /**
   * Calculate the median of a data column containing numerical data. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights are thrown out before the calculation is performed.
   * The option None is returned when the total weight is 0.
   *
   * @param dataColumnIndex column index
   * @param weightsColumnIndexOption  Option for index of column providing  weights. Must be numerical data.
   * @param rowRDD RDD of input rows
   * @return the  median of the column (as a double)
   */
  def columnMedian(dataColumnIndex: Int,
                   weightsColumnIndexOption: Option[Int],
                   rowRDD: RDD[Row]): ColumnMedianReturn = {

    val dataWeightPairs: RDD[(Double, Double)] = getDoubleWeightPairs(dataColumnIndex, weightsColumnIndexOption, rowRDD)

    val orderStatistics = new OrderStatistics[Double](dataWeightPairs)

    ColumnMedianReturn(orderStatistics.medianOption)
  }

  /**
   * Calculate summary statistics of data column, possibly weighted by an optional weights column.
   *
   * @param dataColumnIndex Index of column providing the data. Must be numerical data.
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param rowRDD RDD of input rows
   * @return summary statistics of the column
   */
  def columnSummaryStatistics(dataColumnIndex: Int,
                              weightsColumnIndexOption: Option[Int],
                              rowRDD: RDD[Row]): ColumnSummaryStatisticsReturn = {

    val dataWeightPairs: RDD[(Double, Double)] = getDoubleWeightPairs(dataColumnIndex, weightsColumnIndexOption, rowRDD)

    val stats = new NumericalStatistics(dataWeightPairs)

    ColumnSummaryStatisticsReturn(mean = stats.weightedMean,
      geometricMean = stats.weightedGeometricMean,
      variance = stats.weightedVariance,
      standardDeviation = stats.weightedStandardDeviation,
      mode = stats.weightedMode,
      meanConfidenceLowerBound = stats.meanConfidenceLower,
      meanConfidenceUpperBound = stats.meanConfidenceUpper,
      minimum = stats.min,
      maximum = stats.max,
      positiveWeightCount = stats.count,
      nonPositiveWeightCount = stats.nonPositiveWeightCount)
  }

  /**
   * Calculate full statistics of data column, possibly weighted by an optional weights column.
   *
   * @param dataColumnIndex Index of column providing the data. Must be numerical data.
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param rowRDD RDD of input rows
   * @return  statistics of the column
   */
  def columnFullStatistics(dataColumnIndex: Int,
                           weightsColumnIndexOption: Option[Int],
                           rowRDD: RDD[Row]): ColumnFullStatisticsReturn = {

    val dataWeightPairs: RDD[(Double, Double)] = getDoubleWeightPairs(dataColumnIndex, weightsColumnIndexOption, rowRDD)

    val stats = new NumericalStatistics(dataWeightPairs)

    ColumnFullStatisticsReturn(mean = stats.weightedMean,
      geometricMean = stats.weightedGeometricMean,
      variance = stats.weightedVariance,
      standardDeviation = stats.weightedStandardDeviation,
      skewness = stats.weightedSkewness,
      kurtosis = stats.weightedKurtosis,
      mode = stats.weightedMode,
      meanConfidenceLowerBound = stats.meanConfidenceLower,
      meanConfidenceUpperBound = stats.meanConfidenceUpper,
      minimum = stats.min,
      maximum = stats.max,
      positiveWeightCount = stats.count,
      nonPositiveWeightCount = stats.nonPositiveWeightCount)
  }

  private def getDataWeightPairs(dataColumnIndex: Int,
                                 weightsColumnIndexOption: Option[Int],
                                 rowRDD: RDD[Row]): RDD[(Any, Double)] = {

    val dataRDD: RDD[Any] = rowRDD.map(row => row(dataColumnIndex))

    val weighted = !weightsColumnIndexOption.isEmpty

    val weightsRDD = if (weighted) rowRDD.map(row => doubleConversion(row(weightsColumnIndexOption.get))) else null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, 1.toDouble))
  }

  private def doubleConversion(x: Any) = {
    if (x.isInstanceOf[Int]) {
      x.asInstanceOf[Int].toDouble
    }
    else if (x.isInstanceOf[Long]) {
      x.asInstanceOf[Long].toDouble
    }
    else if (x.isInstanceOf[Float]) {
      x.asInstanceOf[Float].toDouble
    }
    else if (x.isInstanceOf[Double]) {
      x.asInstanceOf[Double]
    }
    else {
      throw new IllegalArgumentException("Cannot convert " + x + " to a double.")
    }
  }

  private def getDoubleWeightPairs(dataColumnIndex: Int,
                                   weightsColumnIndexOption: Option[Int],
                                   rowRDD: RDD[Row]): RDD[(Double, Double)] = {

    val dataRDD: RDD[Double] = rowRDD.map(row => doubleConversion(row(dataColumnIndex)))

    val weighted = !weightsColumnIndexOption.isEmpty

    val weightsRDD =
      if (weighted) rowRDD.map(row => doubleConversion(weightsColumnIndexOption.get)) else null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, 1.toDouble))
  }
}
