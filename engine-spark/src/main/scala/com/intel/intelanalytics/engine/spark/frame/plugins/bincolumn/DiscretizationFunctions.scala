//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn

import com.intel.intelanalytics.domain.schema.DataTypes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.math._

//implicit conversion for PairRDD

import org.apache.spark.SparkContext._

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object DiscretizationFunctions extends Serializable {
  /**
   * Bin column at index using equal width binning.
   *
   * Determine cutoffs by finding upper/lower bounds, then map each input rdd column value to a bin number
   * based on the cutoff ranges.
   *
   * @param index column index
   * @param numBins requested number of bins
   * @param rdd RDD that contains the column for binning
   * @return new RDD with binned column appended
   */
  def binEqualWidth(index: Int, numBins: Int, rdd: RDD[Row]): RddWithCutoffs = {
    val cutoffs: Array[Double] = getBinEqualWidthCutoffs(index, numBins, rdd)

    // map each data element to its bin id, using cutoffs index as bin id
    val binnedColumnRdd = binColumns(index, cutoffs.toList, lowerInclusive = true, strictBinning = false, rdd)
    new RddWithCutoffs(cutoffs, binnedColumnRdd)
  }

  /**
   * Bin column at index using list of cutoff values
   * @param index column index
   * @param cutoffs Array containing the cutoff for each bin must be monotonically increasing
   * @param lowerInclusive if true the lowerbound of the bin will be inclusive while the upperbound is exclusive if false it is the opposite
   * @param strictBinning if true values smaller than the first bin or larger than the last bin will not be given a bin.
   *                      if false smaller vales will be in the first bin and larger values will be in the last
   * @param rdd RDD that contains the column for binning
   * @return new RDD with binned column appended
   */
  def binColumns(index: Int, cutoffs: List[Double], lowerInclusive: Boolean, strictBinning: Boolean, rdd: RDD[Row]): RDD[Row] = {
    val min: Int = 0
    val max: Int = cutoffs.length - 2
    rdd.map { row: Row =>
      val element = DataTypes.toDouble(row(index))
      //if lower than first cutoff
      val binIndex = binElement(element, cutoffs, lowerInclusive, strictBinning)
      new GenericRow(row.toArray :+ binIndex)
    }
  }

  /**
   * Bin element using list of cutoff values
   * @param element Element to bin
   * @param cutoffs Array containing the cutoff for each bin must be monotonically increasing
   * @param lowerInclusive if true the lowerbound of the bin will be inclusive while the upperbound is exclusive if false it is the opposite
   * @param strictBinning if true values smaller than the first bin or larger than the last bin will not be given a bin.
   *                      if false smaller vales will be in the first bin and larger values will be in the last
   * @return bin index
   */
  def binElement(element: Double, cutoffs: List[Double], lowerInclusive: Boolean, strictBinning: Boolean): Int = {
    val min: Int = 0
    val max: Int = cutoffs.length - 2
    //if lower than first cutoff
    val binIndex: Int =
      // if lower than first cutoff
      if (element < cutoffs(0))
        if (strictBinning) -1 else min
      // if larger than last cutoff
      else if (cutoffs.last < element)
        if (strictBinning) -1 else max
      else if (lowerInclusive) {
        if ((element - cutoffs.last).abs < 0.00001d)
          max
        else
          bSearchRangeLowerInclusive(element, cutoffs, min, max)
      }
      else {
        if ((element - cutoffs(0)).abs < 0.00001d)
          min
        else
          bSearchRangeUpperInclusive(element, cutoffs, min, max)
      }

    binIndex
  }

  /**
   * determine which bin the element should be grouped into if including the lower bound as part of the bin.
   * The bin corresponds to the area between two values.
   * For an element 5 with a cutoff list of [1,4,7,10] the bin would be 2, being between 4 and 7.
   *
   * @param element element to bin
   * @param cutoffs list of cutoffs
   * @param min lowest bin to search
   * @param max highest bin to search
   * @return -1 if item is out of bounds, bin number if found
   */
  def bSearchRangeLowerInclusive(element: Double, cutoffs: List[Double], min: Int, max: Int): Int = {
    if (max < min) // number not in cutoffs
      return -1
    val mid = (max + min) / 2
    if (cutoffs(mid + 1) <= element)
      bSearchRangeLowerInclusive(element, cutoffs, mid + 1, max)
    else if (element < cutoffs(mid))
      bSearchRangeLowerInclusive(element, cutoffs, min, mid - 1)
    else
      mid
  }

  /**
   * determine which bin the element should be grouped into if including the upper bound as part of the bin.
   * The bin corresponds to the area between two values.
   * For an element 5 with a cutoff list of [1,4,7,10] the bin would be 2, being between 4 and 7.
   *
   * @param element element to bin
   * @param cutoffs list of cutoffs
   * @param min lowest bin to search
   * @param max highest bin to search
   * @return -1 if item is out of bounds, bin number if found
   */
  def bSearchRangeUpperInclusive(element: Double, cutoffs: List[Double], min: Int, max: Int): Int = {
    if (max < min) // number not in cutoffs
      return -1
    val mid = (max + min) / 2
    if (cutoffs(mid + 1) < element)
      bSearchRangeUpperInclusive(element, cutoffs, mid + 1, max)
    else if (element <= cutoffs(mid))
      bSearchRangeUpperInclusive(element, cutoffs, min, mid - 1)
    else
      mid
  }

  /**
   * Determine the range cutoffs for a binned column by finding upper/lower bounds, then map each input
   * rdd column value to a bin number based on the cutoff ranges.
   * @param index index of the binned column
   * @param numBins number of bins to divide the column into
   * @param rdd rdd containing the column
   * @return an array containing the cutoffs
   */
  def getBinEqualWidthCutoffs(index: Int, numBins: Int, rdd: RDD[Row]): Array[Double] = {
    // try parsing column as pairs of doubles
    val pairedRdd = try {
      rdd.map { row =>
        val value = java.lang.Double.parseDouble(row(index).toString)
        (value, value)
      }.distinct()
    }
    catch {
      case cce: NumberFormatException => throw new NumberFormatException("Column values cannot be binned: " + cce.toString)
    }

    // find the minimum and maximum values in the column RDD
    val min: Double = pairedRdd.sortByKey().first()._1
    val max: Double = pairedRdd.sortByKey(ascending = false).first()._1

    // determine bin width and cutoffs
    val binWidth = (max - min) / numBins.toDouble

    if (binWidth != 0) {
      (min to max by binWidth).toArray
    }
    else {
      List(min, min).toArray
    }
  }

  /**
   * Bin column at index using equal depth binning.
   *
   * For n bins of a column C of length m, the bin number is determined by:
   * ceiling(n * f(C) / m)
   * where f is a tie-adjusted ranking function over values of C.  If there are multiple of the same value in C, then
   * their tie-adjusted rank is the average of their ordered rank values.
   *
   * @param index column index
   * @param numBins requested number of bins
   * @param weightedIndex column index representing the weight of a value
   * @param rdd RDD for binning
   * @return new RDD with binned column appended
   */
  def binEqualDepth(index: Int, numBins: Int, weightedIndex: Option[Int], rdd: RDD[Row]): RddWithCutoffs = {
    val binNumberMap: Map[Double, Int] = getBinEqualDepthNumberMap(index, numBins, weightedIndex, rdd)
    val sortedTuples: List[(Int, Map[Double, Int])] = binNumberMap.groupBy(_._2).toList.sortBy(_._1)
    val cutoffs = sortedTuples.map(_._2.keys.min) :+ sortedTuples.last._2.keys.max
    val binnedRdd = binUsingBroadcast(index, binNumberMap, rdd)
    new RddWithCutoffs(cutoffs.toArray, binnedRdd)
  }

  def binUsingBroadcast(index: Int, binNumberMap: Map[Double, Int], rdd: RDD[Row]): RDD[Row] = {
    val broadcastBinMap = rdd.sparkContext.broadcast(binNumberMap)
    rdd.map(row => new GenericRow(row.toArray :+ (broadcastBinMap.value.get(DataTypes.toDouble(row(index))).get - 1).asInstanceOf[Any]))
  }

  def getBinEqualDepthNumberMap(index: Int, numBins: Int, weightedIndex: Option[Int], rdd: RDD[Row]): Map[Double, Int] = {
    // try creating RDD[Double] from column
    val columnRdd = rdd.map(row => (DataTypes.toDouble(row(index)),
      weightedIndex match {
        case Some(i) => math.max(DataTypes.toDouble(row(i)), 1)
        case None => 1.0
      }))
    columnRdd.cache()

    // assign a rank to each distinct element
    val numElements = columnRdd.values.sum
    val rankedElementRdd = assignElementRanks(columnRdd)

    // compute the bin number
    val binNumberMap = assignBinNumbers(rankedElementRdd, numElements, numBins).collect().toMap
    binNumberMap
  }

  /**
   * Sort elements in column by ascending order and assign rank
   *
   * @param columnRdd RDD of column values with their weight
   * @return RDD of column and rank
   */
  private def assignElementRanks(columnRdd: RDD[(Double, Double)]): RDD[(Double, Double)] = {
    val elementFrequencyRdd = columnRdd.reduceByKey((a, b) => a + b).sortByKey()
    elementFrequencyRdd.cache()

    // Use broadcast variable to determine starting rank for each partition
    val initialPartitionRank = getInitialPartitionRanks(elementFrequencyRdd)
    val broadcastPartitionRanks = columnRdd.sparkContext.broadcast(initialPartitionRank)

    val rankedRdd = elementFrequencyRdd.mapPartitionsWithIndex((i, iter) => {
      var rank = broadcastPartitionRanks.value(i)
      iter.map {
        case (element, frequency) =>
          // Using while loop instead of range because Scala ranges cannot cope with values that exceed Max.Int
          var rankCounter = rank;
          var rankSum = 0.0;
          while (rankCounter < (rank + frequency)) {
            rankSum = rankSum + rankCounter
            rankCounter = rankCounter + 1
          }
          val avgRank = BigDecimal(rankSum) / BigDecimal(if (frequency > 0) frequency else 1)
          rank += frequency
          (element, avgRank.toDouble)
      }
    })
    elementFrequencyRdd.unpersist()
    rankedRdd
  }

  /**
   * Assign ranked numbers to bins
   *
   * @param rankedElementRdd RDD of sorted elements ranked in ascending order
   * @param numElements Total number of elements
   * @param numBins Requested number of bins
   * @return  RDD of elements and corresponding bin number
   */
  private def assignBinNumbers(rankedElementRdd: RDD[(Double, Double)], numElements: Double, numBins: Int): RDD[(Double, Int)] = {
    val binnedElementRdd = rankedElementRdd.map {
      case (element, rank) =>
        val bin = ceil((numBins * rank) / numElements).toInt
        (element, bin)
    }
    binnedElementRdd.cache()

    // shift the bin numbers so that they are contiguous values
    val sortedBinRanks = binnedElementRdd.map { case (element, bin) => bin }.distinct().sortBy(bin => bin).zipWithIndex().collect()
    val broadcastSortedBins = rankedElementRdd.sparkContext.broadcast(sortedBinRanks.toMap)

    val rankedBinRdd = binnedElementRdd.map {
      case (element, bin) =>
        val binNumber = broadcastSortedBins.value
          .getOrElse(bin, throw new RuntimeException(s"Unable to find ranking for bin${bin}"))
        (element, (binNumber + 1).toInt)
    }
    binnedElementRdd.unpersist()
    rankedBinRdd
  }

  /**
   * Get initial ranks for each partition.
   *
   * The initial rank in each partition is the sum of elements in the preceding partitions
   *
   * @param elementFrequencyRdd RDD of elements and frequency
   * @return Array of initial ranks for each partition in RDD
   */
  def getInitialPartitionRanks(elementFrequencyRdd: RDD[(Double, Double)]): Array[Double] = {
    elementFrequencyRdd.mapPartitions(iter => {
      var sum = 0.0
      iter.foreach { case (element, frequency) => sum += frequency }
      Seq(sum).toIterator
    }).collect().scanLeft(1.0)(_ + _)
  }
}
