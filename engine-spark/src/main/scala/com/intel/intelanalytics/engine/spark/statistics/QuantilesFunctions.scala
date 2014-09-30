//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.algorithm.{ QuantileComposingElement, QuantileTarget, Quantile }
import com.intel.intelanalytics.domain.schema.DataTypes
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.intel.intelanalytics.engine.spark.frame.MiscFrameFunctions

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

object QuantilesFunctions extends Serializable {

  /**
   * Calculate quantile values
   * @param rdd input rdd
   * @param quantiles seq of quantiles to find value for
   * @param columnIndex the index of column to calculate quantile
   * @param dataType data type of the column
   *
   * Currently calculate quantiles with weight average. n be the number of total elements which is ordered,
   * T th quantile can be calculated in the following way.
   * n * T / 100 = i + j   i is the integer part and j is the fractional part
   * The quantile is Xi * (1- j) + Xi+1 * j
   *
   * Calculating a list of quantiles follows the following process:
   * 1. calculate components for each quantile. If T th quantile is Xi * (1- j) + Xi+1 * j, output
   * (i, (1 - j)), (i + 1, j).
   * 2. transform the components. Take component (i, (1 - j)) and transform to (i, (T, 1 - j)), where (T, 1 -j) is
   * a quantile target for element i. Create mapping i -> seq(quantile targets)
   * 3. iterate through all elements in each partition. for element i, find sequence of quantile targets from
   * the mapping created earlier. emit (T, i * (1 - j))
   * 4. reduce by key, which is quantile. Sum all partial results to get the final quantile values.
   */
  def quantiles(rdd: RDD[Row], quantiles: Seq[Double], columnIndex: Int, dataType: DataType): Seq[Quantile] = {
    val totalRows = rdd.count()
    val pairRdd = rdd.map(row => MiscFrameFunctions.createKeyValuePairFromRow(row, List(columnIndex))).map { case (keyColumns, data) => (keyColumns(0).toString.toDouble, data) }
    val sorted = pairRdd.asInstanceOf[RDD[(Double, Row)]].sortByKey(true)

    val quantileTargetMapping = getQuantileTargetMapping(totalRows, quantiles)
    val sumsAndCounts: Map[Int, (Int, Int)] = MiscFrameFunctions.getPerPartitionCountAndAccumulatedSum(sorted)

    //this is the first stage of calculating quantile
    //generate data that has keys as quantiles and values as column data times weight
    val quantilesComponentsRDD = sorted.mapPartitionsWithIndex((partitionIndex, rows) => {
      var rowIndex: Long = (if (partitionIndex == 0) 0 else sumsAndCounts(partitionIndex - 1)._2) + 1
      val perPartitionResult = ListBuffer[(Double, BigDecimal)]()

      for (row <- rows) {
        if (quantileTargetMapping.contains(rowIndex)) {
          val targets: Seq[QuantileTarget] = quantileTargetMapping(rowIndex)

          for (quantileTarget <- targets) {
            val value = row._1
            val numericVal = DataTypes.toBigDecimal(value)
            perPartitionResult += ((quantileTarget.quantile, numericVal * quantileTarget.weight))
          }
        }

        rowIndex = rowIndex + 1
      }

      perPartitionResult.toIterator
    })

    quantilesComponentsRDD.reduceByKey(_ + _).sortByKey(true).collect().map { case (quantile, value) => Quantile(quantile, value) }
  }

  /**
   * calculate and return elements for calculating quantile
   * For example, 25th quantile out of 10 rows(X1, X2, X3, ... X10) will be
   * 0.5 * x2 + 0.5 * x3. The method will return x2 and x3 with weight as 0.5
   *
   * For whole quantile calculation process, please refer to doc of method calculateQuantiles
   * @param totalRows
   * @param quantile
   */
  def getQuantileComposingElements(totalRows: Long, quantile: Double): Seq[QuantileComposingElement] = {
    val position = (BigDecimal(quantile) * totalRows) / 100
    var integerPosition = position.toLong
    val fractionPosition = position - integerPosition

    val result = mutable.ListBuffer[QuantileComposingElement]()

    val addQuantileComposingElement = (position: Long, quantile: Double, weight: BigDecimal) => {
      //element starts from 1. therefore X0 equals X1
      if (weight > 0)
        result += QuantileComposingElement(if (position != 0) position else 1, QuantileTarget(quantile, weight))
    }

    addQuantileComposingElement(integerPosition, quantile, 1 - fractionPosition)
    addQuantileComposingElement(integerPosition + 1, quantile, fractionPosition)
    result.toSeq
  }

  /**
   * Calculate mapping between an element's position and Seq of quantiles that the element can contribute to
   * @param totalRows total number of rows in the data
   * @param quantiles Sequence of quantiles to search
   *
   * For whole quantile calculation process, please refer to doc of method calculateQuantiles
   */
  def getQuantileTargetMapping(totalRows: Long, quantiles: Seq[Double]): Map[Long, Seq[QuantileTarget]] = {

    val composingElements: Seq[QuantileComposingElement] = quantiles.flatMap(quantile => getQuantileComposingElements(totalRows, quantile))

    val mapping = mutable.Map[Long, ListBuffer[QuantileTarget]]()
    for (element <- composingElements) {
      val elementIndex: Long = element.index

      if (!mapping.contains(elementIndex))
        mapping(elementIndex) = ListBuffer[QuantileTarget]()

      mapping(elementIndex) += element.quantileTarget
    }

    //for each element's quantile targets, convert from ListBuffer to Seq
    //convert the map to immutable map
    mapping.map { case (elementIndex, targets) => (elementIndex, targets.toSeq) }.toMap
  }

}
