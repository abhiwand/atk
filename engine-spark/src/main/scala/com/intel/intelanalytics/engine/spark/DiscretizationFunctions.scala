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

package com.intel.intelanalytics.engine.spark

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._

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
  def binEqualWidth(index: Int, numBins: Int, rdd: RDD[Row]): RDD[Row] = {
    // TODO: Need consider how cutoffs/binSizes are going to be returned (if at all)

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
    val max: Double = pairedRdd.sortByKey(false).first()._1

    // determine bin width and cutoffs
    val binWidth = (max - min) / numBins.toDouble

    val cutoffs = if (binWidth != 0) {
      (min to max by binWidth).toArray
    }
    else {
      List(min, min).toArray
    }

    // map each data element to its bin id, using cutoffs index as bin id
    val binnedColumnRdd = rdd.map { row =>
      var binIndex = 0
      var working = true
      val element = java.lang.Double.parseDouble(row(index).toString)
      do {
        for (i <- 0 to cutoffs.length - 2) {
          // inclusive upper-bound on last cutoff range
          if ((i == cutoffs.length - 2) && (element - cutoffs(i) >= 0)) {
            binIndex = i
            working = false
          }
          else if ((element - cutoffs(i) >= 0) && (element - cutoffs(i + 1) < 0)) {
            binIndex = i
            working = false
          }
        }
      } while (working)
      row :+ binIndex.asInstanceOf[Any]
    }
    binnedColumnRdd
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
   * @param rdd RDD for binning
   * @return new RDD with binned column appended
   */
  def binEqualDepth(index: Int, numBins: Int, rdd: RDD[Row]): RDD[Row] = {
    import scala.math.ceil

    // try creating RDD[Double] from column
    val columnRdd = try {
      rdd.map(row => java.lang.Double.parseDouble(row(index).toString))
    }
    catch {
      case cce: NumberFormatException => throw new NumberFormatException("Column values cannot be binned: " + cce.toString)
    }

    val numElements = columnRdd.count()

    // assign a rank to each distinct element
    val pairedRdd = columnRdd.groupBy(element => element).map(pairs => (pairs._1, pairs._2.size)).sortByKey()

    // Need to go through values sequentially, but this creates an issue with Spark and multiple partitions
    // Option 1: use outside var counter...but each partition gets a fresh copy (no good)
    // Option 2: use Spark accumulator...but nondeterministic order of access among partitions (no good)
    // Option 3: convert RDD to Array for sequential operations and back to RDD otherwise (works fine but inefficient)
    // TODO: Option 4: ??? (find better way that avoids iterating over potentially long Arrays)
    val pairedArray = pairedRdd.collect()

    // the following will fail for columns that contain more than Int.MaxValue of a specific value
    var rank = 1
    val rankedArray = try {
      pairedArray.map { value =>
        val avgRank = BigDecimal((BigInt(rank) to BigInt(rank + (value._2 - 1))).foldLeft(BigInt("0"))(_ + _)) / BigDecimal(value._2)
        rank += value._2
        (value._1, avgRank.toDouble)
      }
    }
    catch {
      case iae: IllegalArgumentException => throw new IllegalArgumentException("More than Int.MaxValue of specific column value" + iae.toString)
    }

    val rankedRdd = rdd.sparkContext.parallelize(rankedArray)

    // compute the bin number
    val binnedRdd = rankedRdd.map { valueRank =>
      val bin = ceil((numBins * valueRank._2) / numElements.asInstanceOf[Double]).asInstanceOf[Long]
      (valueRank._1, bin)
    }

    // shift the bin numbers so that they are contiguous values
    val sortedBinnedRdd = binnedRdd.groupBy(valueBin => valueBin._2).sortByKey()

    val sortedBinnedArray = sortedBinnedRdd.collect()

    rank = 1
    val shiftedArray = sortedBinnedArray.flatMap { binMappings =>
      val valuePairs = binMappings._2.map(valueBin => (valueBin._1, rank))
      rank += 1
      valuePairs
    }

    val binMap = shiftedArray.toMap
    rdd.map(row => row :+ (binMap.get(java.lang.Double.parseDouble(row(index).toString)).get - 1).asInstanceOf[Any])
  }

}
