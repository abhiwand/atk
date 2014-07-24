package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.frame.{ ColumnMedianReturn, ColumnModeReturn, ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }
import com.intel.intelanalytics.engine.spark.statistics.numericalstatistics._
import spray.json._
import DefaultJsonProtocol._
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes

/**
 * Provides functions for taking statistics on column data.
 */
private[spark] object ColumnStatistics extends Serializable {

  /**
   * Calculate (weighted) mode of a data column, the weight of the mode, and the total weight of the column.
   * A mode is a value that has maximum weight. Ties are resolved arbitrarily.
   * Values with non-positive weights (including NaNs and infinite values) are thrown out before the calculation is
   * performed.
   *
   * When the total weight is 0, the option None is given for the mode and the weight of the mode.
   *
   * @param dataColumnIndex Index of the column providing data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @return The mode of the column (as a string), the weight of the mode, and the total weight of the data.
   */
  def columnMode(dataColumnIndex: Int,
                 dataType: DataType,
                 weightsColumnIndexOption: Option[Int],
                 weightsTypeOption: Option[DataType],
                 rowRDD: RDD[Row]): ColumnModeReturn = {

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, rowRDD)

    val frequencyStatistics = new FrequencyStatistics(dataWeightPairs)

    val modeJsValue: JsValue = if (frequencyStatistics.mode.isEmpty) {
      None.asInstanceOf[Option[String]].toJson
    }
    else {
      dataType.json(frequencyStatistics.mode.get)
    }

    ColumnModeReturn(modeJsValue, frequencyStatistics.weightOfMode, frequencyStatistics.totalWeight)

  }

  /**
   * Calculate the median of a data column containing numerical data. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed. The option None is returned when the total weight is 0.
   *
   * @param dataColumnIndex Index of the data column.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption  Option for index of column providing  weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @return The  median of the column.
   */
  def columnMedian(dataColumnIndex: Int,
                   dataType: DataType,
                   weightsColumnIndexOption: Option[Int],
                   weightsTypeOption: Option[DataType],
                   rowRDD: RDD[Row]): ColumnMedianReturn = {

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, rowRDD)

    implicit val ordering: Ordering[Any] = new NumericalOrdering(dataType)

    val orderStatistics = new OrderStatistics[Any](dataWeightPairs)

    val medianReturn: JsValue = if (orderStatistics.medianOption.isEmpty) {
      None.asInstanceOf[Option[Double]].toJson
    }
    else {
      dataType.json(orderStatistics.medianOption.get)
    }
    ColumnMedianReturn(medianReturn)
  }

  private class NumericalOrdering(dataType: DataType) extends Ordering[Any] {
    override def compare(x: Any, y: Any): Int = {
      dataType.asDouble(x).compareTo(dataType.asDouble(y))
    }
  }

  /**
   * Calculate summary statistics of data column, possibly weighted by an optional weights column.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed, however, they are logged as "bad rows" (when a row contain a datum or a weight that is not a finite
   * number) or as "non positive weight" (when a row's weight entry is <= 0).
   *
   * @param dataColumnIndex Index of column providing the data. Must be numerical data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @return Summary statistics of the column.
   */
  def columnSummaryStatistics(dataColumnIndex: Int,
                              dataType: DataType,
                              weightsColumnIndexOption: Option[Int],
                              weightsTypeOption: Option[DataType],
                              rowRDD: RDD[Row]): ColumnSummaryStatisticsReturn = {

    val dataWeightPairs: RDD[(Double, Double)] =
      getDoubleWeightPairs(dataColumnIndex, dataType, weightsColumnIndexOption, weightsTypeOption, rowRDD)

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
      positiveWeightCount = stats.positiveWeightCount,
      nonPositiveWeightCount = stats.nonPositiveWeightCount,
      badRowCount = stats.badRowCount,
      validDataWeightPairCount = stats.goodRowCount)
  }

  /**
   * Calculate full statistics of data column, possibly weighted by an optional weights column.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed, however, they are logged as "bad rows" (when a row contain a datum or a weight that is not a finite
   * number) or as "non positive weight" (when a row's weight entry is <= 0).
   *
   * @param dataColumnIndex Index of column providing the data. Must be numerical data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @return  Full statistics of the column.
   */
  def columnFullStatistics(dataColumnIndex: Int,
                           dataType: DataType,
                           weightsColumnIndexOption: Option[Int],
                           weightsTypeOption: Option[DataType],
                           rowRDD: RDD[Row]): ColumnFullStatisticsReturn = {

    val dataWeightPairs: RDD[(Double, Double)] =
      getDoubleWeightPairs(dataColumnIndex, dataType, weightsColumnIndexOption, weightsTypeOption, rowRDD)

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
      positiveWeightCount = stats.positiveWeightCount,
      nonPositiveWeightCount = stats.nonPositiveWeightCount,
      badRowCount = stats.badRowCount,
      validDataWeightPairCount = stats.goodRowCount)
  }

  private def getDataWeightPairs(dataColumnIndex: Int,
                                 weightsColumnIndexOption: Option[Int],
                                 weightsTypeOption: Option[DataType],
                                 rowRDD: RDD[Row]): RDD[(Any, Double)] = {

    val dataRDD: RDD[Any] = rowRDD.map(row => row(dataColumnIndex))

    val weighted = !weightsColumnIndexOption.isEmpty

    if (weightsColumnIndexOption.nonEmpty && weightsTypeOption.isEmpty) {
      throw new IllegalArgumentException("Cannot specify weights column without specifying its datatype.")
    }

    val weightsRDD = if (weighted)
      rowRDD.map(row => weightsTypeOption.get.asDouble(row(weightsColumnIndexOption.get)))
    else
      null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, 1.toDouble))
  }

  private def getDoubleWeightPairs(dataColumnIndex: Int,
                                   dataType: DataType,
                                   weightsColumnIndexOption: Option[Int],
                                   weightsTypeOption: Option[DataType],
                                   rowRDD: RDD[Row]): RDD[(Double, Double)] = {

    val dataRDD: RDD[Double] = rowRDD.map(row => dataType.asDouble(row(dataColumnIndex)))

    val weighted = !weightsColumnIndexOption.isEmpty

    if (weightsColumnIndexOption.nonEmpty && weightsTypeOption.isEmpty) {
      throw new IllegalArgumentException("Cannot specify weights column without specifying its datatype.")
    }

    val weightsRDD = if (weighted)
      rowRDD.map(row => weightsTypeOption.get.asDouble(row(weightsColumnIndexOption.get)))
    else
      null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, 1.toDouble))
  }

}
