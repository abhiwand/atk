package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.frame.{ ColumnFullStatisticsReturn, ColumnSummaryStatisticsReturn }

private[spark] object ColumnStatistics extends Serializable {

  /**
   * Calculate (weighted) mode of column at index.
   *
   * @param dataColumnIndex column index
   * @param weightsColumnIndexOption Option for index of column providing  weights. Must be numerical data.
   * @param rowRDD RDD of input rows
   * @return the  mode of the column (as a string)
   */
  def columnMode(dataColumnIndex: Int,
                 weightsColumnIndexOption: Option[Int],
                 rowRDD: RDD[Row]): (String, Double, Double) = {

    val dataWeightPairs: RDD[(String, Double)] = getStringWeightPairs(dataColumnIndex, weightsColumnIndexOption, rowRDD)

    val frequencyStatistics = new FrequencyStatistics(dataWeightPairs, "no items found")
    frequencyStatistics.modeItsWeightTotalWeightTriple
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

    /*
    if (operation equals "MEDIAN") {
      val count: Long = dataRDD.stats().count
      val medianIndex: Long = count / 2

      val workingRDD = if (weighted) {
        dataRDD.zip(weightsRDD).map({ case (d, w) => d * w })
      }
      else {
        dataRDD
      }

      val sortedRdd = workingRDD.map(x => (x, x)).sortByKey(ascending = true)

      val partitionCounts: Array[Int] = sortedRdd.glom().map(a => a.length).collect()

      var partitionIndex = 0
      var totalCountSeenPrevious = 0

      while (medianIndex >= totalCountSeenPrevious + partitionCounts.apply(partitionIndex)) {
        partitionIndex += 1
        totalCountSeenPrevious += partitionCounts.apply(partitionIndex)
      }

      val indexOfMedianInsidePartition = (medianIndex - totalCountSeenPrevious).toInt

      def partitionSelector(index: Int, partitionIterator: Iterator[(Double, Double)]) = {
        if (index == partitionIndex) partitionIterator else Iterator[(Double, Double)]()
      }

      val partRdd = sortedRdd.mapPartitionsWithIndex(partitionSelector, true)

      val segmentContainingMedian = partRdd.collect

      segmentContainingMedian.apply(indexOfMedianInsidePartition)._1
    }
    */

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    numericalStatistics.summaryStatistics
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

    val numericalStatistics = new NumericalStatistics(dataWeightPairs)

    numericalStatistics.fullStatistics
  }

  private def getDoubleWeightPairs(dataColumnIndex: Int,
                                   weightsColumnIndexOption: Option[Int],
                                   rowRDD: RDD[Row]): RDD[(Double, Double)] = {

    val dataRDD = try {
      rowRDD.map(row => java.lang.Double.parseDouble(row(dataColumnIndex).toString))
    }
    catch {
      case cce: NumberFormatException => throw new NumberFormatException("Column values are non-numeric :"
        + cce.toString)
    }

    val weighted = !weightsColumnIndexOption.isEmpty

    val weightsRDD = if (weighted) {
      try {
        rowRDD.map(row => java.lang.Double.parseDouble(row(weightsColumnIndexOption.get).toString))
      }
      catch {
        case cce: NumberFormatException => throw new NumberFormatException("Column values are non-numeric :"
          + cce.toString)
      }
    }
    else {
      null
    }

    val dataWeightPairs = if (weighted) {
      dataRDD.zip(weightsRDD)
    }
    else {
      dataRDD.map(x => (x, 1.toDouble))
    }
    dataWeightPairs
  }

  private def getStringWeightPairs(dataColumnIndex: Int,
                                   weightsColumnIndexOption: Option[Int],
                                   rowRDD: RDD[Row]): RDD[(String, Double)] = {

    val dataRDD: RDD[String] =
      try {
        rowRDD.map(row => row(dataColumnIndex).toString)
      }
      catch {
        case cce: NumberFormatException => throw new NumberFormatException("Column values  from cannot be parsed :"
          + cce.toString)
      }

    val weighted = !weightsColumnIndexOption.isEmpty

    val weightsRDD = if (weighted) {
      try {
        rowRDD.map(row => java.lang.Double.parseDouble(row(weightsColumnIndexOption.get).toString))
      }
      catch {
        case cce: NumberFormatException => throw new NumberFormatException("Column values cannot be used as weights for "
          + "mode calculation:" + cce.toString)
      }
    }
    else {
      null
    }

    val dataWeightPairs = if (weighted) {
      dataRDD.zip(weightsRDD)
    }
    else {
      dataRDD.map(x => (x, 1.toDouble))
    }
    dataWeightPairs
  }
}
