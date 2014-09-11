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

import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.reflect.ClassTag

import scala.Some
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, RDDJoinParam }
import com.intel.intelanalytics.algorithm.{ Quantile, QuantileTarget, QuantileComposingElement }
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import scala.math.pow
import scala.reflect.ClassTag
import com.intel.intelanalytics.domain.frame.FrameGroupByColumn
import spray.json.JsObject

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

private[spark] object SparkOps extends Serializable {

  /**
   * take an input RDD and return another RDD which contains the subset of the original contents
   * @param rdd input RDD
   * @param offset rows to be skipped before including rows in the new RDD
   * @param count total rows to be included in the new RDD
   * @param limit limit on number of rows to be included in the new RDD
   */
  def getPagedRdd[T: ClassTag](rdd: RDD[T], offset: Long, count: Int, limit: Int): RDD[T] = {

    val sumsAndCounts = SparkOps.getPerPartitionCountAndAccumulatedSum(rdd)
    val capped = limit match {
      case -1 => count
      case _ => Math.min(count, limit)
    }
    //Start getting rows. We use the sums and counts to figure out which
    //partitions we need to read from and which to just ignore
    val pagedRdd: RDD[T] = rdd.mapPartitionsWithIndex((i, rows) => {
      val (ct: Int, sum: Int) = sumsAndCounts(i)
      val thisPartStart = sum - ct
      if (sum < offset || thisPartStart >= offset + capped) {
        //println("skipping partition " + i)
        Iterator.empty
      }
      else {
        val start = Math.max(offset - thisPartStart, 0)
        val numToTake = Math.min((capped + offset) - thisPartStart, ct) - start
        //println(s"partition $i: starting at $start and taking $numToTake")
        rows.drop(start.toInt).take(numToTake.toInt)
      }
    })

    pagedRdd
  }

  /**
   * take input RDD and return the subset of the original content
   * @param rdd input RDD
   * @param offset  rows to be skipped before including rows in the result
   * @param count total rows to be included in the result
   * @param limit limit on number of rows to be included in the result
   */
  def getRows[T: ClassTag](rdd: RDD[T], offset: Long, count: Int, limit: Int): Seq[T] = {
    val pagedRdd = getPagedRdd(rdd, offset, count, limit)
    val rows: Seq[T] = pagedRdd.collect()
    rows
  }

  /**
   * Return the count and accumulated sum of rows in each partition
   */
  def getPerPartitionCountAndAccumulatedSum[T](rdd: RDD[T]): Map[Int, (Int, Int)] = {
    //Count the rows in each partition, then order the counts by partition number
    val counts = rdd.mapPartitionsWithIndex(
      (i: Int, rows: Iterator[T]) => Iterator.single((i, rows.size)))
      .collect()
      .sortBy(_._1)

    //Create cumulative sums of row counts by partition, e.g. 1 -> 200, 2-> 400, 3-> 412
    //if there were 412 rows divided into two 200 row partitions and one 12 row partition
    val sums = counts.scanLeft((0, 0)) {
      (t1, t2) => (t2._1, t1._2 + t2._2)
    }
      .drop(1) //first one is (0,0), drop that
      .toMap

    //Put the per-partition counts and cumulative counts together
    val sumsAndCounts = counts.map {
      case (part, count) => (part, (count, sums(part)))
    }.toMap
    sumsAndCounts
  }

  /**
   * Load each line from CSV file into an RDD of Row objects.
   * @param ctx SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param skipRows number of rows to skip before beginning parsing
   * @param parserFunction function used for parsing lines into Row objects
   * @param converter function used for converting parsed strings into DataTypes
   * @return  RDD of Row objects
   */
  def loadLines(ctx: SparkContext,
                fileName: String,
                skipRows: Option[Int],
                parserFunction: String => Array[String],
                converter: Array[String] => Array[Any]): RDD[Row] = {
    ctx.textFile(fileName)
      .mapPartitionsWithIndex {
        case (partition, lines) => {
          if (partition == 0) {
            lines.drop(skipRows.getOrElse(0)).map(parserFunction)
          }
          else {
            lines.map(parserFunction)
          }
        }
      }
      .map(converter)
  }

  /**
   * generate 2 tuple instance in order to invoke pairRDD functions
   * @param data row data
   * @param keyIndex index of the key column
   */
  def createKeyValuePairFromRow(data: Array[Any], keyIndex: Seq[Int]): (Seq[Any], Array[Any]) = {

    var key: Seq[Any] = Seq()
    for (i <- keyIndex)
      key = key :+ data(i)

    (key, data)
  }

  /**
   * perform join operation
   * @param left parameter regarding the first dataframe
   * @param right parameter regarding the second dataframe
   * @param how join method
   */
  def joinRDDs(left: RDDJoinParam, right: RDDJoinParam, how: String): RDD[Array[Any]] = {

    val result = how match {
      case "left" => left.rdd.leftOuterJoin(right.rdd).map(t => {
        val rightValues: Option[Array[Any]] = t._2._2
        val leftValues: Array[Any] = t._2._1
        rightValues match {
          case s: Some[Array[Any]] => leftValues ++ s.get
          case None => leftValues ++ (1 to right.columnCount).map(i => null)
        }
      })

      case "right" => left.rdd.rightOuterJoin(right.rdd).map(t => {
        val leftValues: Option[Array[Any]] = t._2._1
        val rightValues: Array[Any] = t._2._2
        leftValues match {
          case s: Some[Array[Any]] => s.get ++ rightValues
          case None => {
            var array: Array[Any] = rightValues
            (1 to left.columnCount).foreach(i => array = null +: array)
            array
          }
        }
      })

      case "outer" => SparkOps.fullOuterJoin(left.rdd, right.rdd).map(t => {
        t._2 match {
          case (Some(leftValues), Some(rightValues)) => { leftValues ++ rightValues }
          case (Some(leftValues), None) => {
            leftValues ++ (1 to right.columnCount).map(i => null)
          }
          case (None, Some(rightValues)) => {
            var array: Array[Any] = rightValues
            (1 to left.columnCount).foreach(i => array = null +: array)
          }
        }
      })

      case "inner" => left.rdd.join(right.rdd).map(t => {
        val leftValues: Array[Any] = t._2._1
        val rightValues: mutable.ArrayOps[Any] = t._2._2
        leftValues ++ rightValues
      })

      case other: String => throw new IllegalArgumentException(s"Method $other not supported. only support left, right, outer and inner.")
    }

    result.asInstanceOf[RDD[Array[Any]]]
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   */
  def fullOuterJoin(left: RDD[(Any, Array[Any])], other: RDD[(Any, Array[Any])]): RDD[(Any, (Option[Array[Any]], Option[Array[Any]]))] = {
    left.cogroup(other).flatMapValues {
      case (vs, Seq()) => vs.map(v => (Some(v), None))
      case (Seq(), ws) => ws.map(w => (None, Some(w)))
      case (vs, ws) => for (v <- vs; w <- ws) yield (Some(v), Some(w))
    }.asInstanceOf[RDD[(Any, (Option[Array[Any]], Option[Array[Any]]))]]
  }

  /**
   * flatten a row by the column with specified column index
   * Eg. for row (1, "dog,cat"), flatten by second column will yield (1,"dog") and (1,"cat")
   * @param index column index
   * @param row row data
   * @param separator separator for splitting
   * @return flattened out row/rows
   */
  def flattenColumnByIndex(index: Int, row: Array[Any], separator: String): Array[Array[Any]] = {
    val splitted = row(index).asInstanceOf[String].split(separator)
    splitted.map(s => {
      val r = row.clone()
      r(index) = s
      r
    })
  }

  /**
   * Flatten RDD by the column with specified column index
   * @param index column index
   * @param separator separator for splitting
   * @param rdd RDD for flattening
   * @return new RDD with column flattened
   */
  def flattenRddByColumnIndex(index: Int, separator: String, rdd: RDD[Row]): RDD[Row] = {
    rdd.flatMap(row => SparkOps.flattenColumnByIndex(index, row, separator))
  }

  /**
   * Compute accuracy of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @return a Double of the model accuracy measure
   */
  def modelAccuracy(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val k = frameRdd.count()
    val t = frameRdd.sparkContext.accumulator[Long](0)

    frameRdd.foreach(row =>
      if (row(labelColumnIndex).toString.equals(row(predColumnIndex).toString)) {
        t.add(1)
      }
    )

    k match {
      case 0 => 0
      case _ => t.value / k.toDouble
    }
  }

  /**
   * Compute precision of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param posLabel the label for a positive instance
   * @return a Double of the model precision measure
   */
  def modelPrecision(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    /**
     * compute precision for binary classifier: TP / (TP + FP)
     */
    def binaryPrecision = {
      val tp = frameRdd.sparkContext.accumulator[Long](0)
      val fp = frameRdd.sparkContext.accumulator[Long](0)

      frameRdd.foreach { row =>
        if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          tp.add(1)
        }
        else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          fp.add(1)
        }
      }
      // if, for example, user specifies pos_label that does not exist, then return 0 for precision
      (tp.value + fp.value) match {
        case 0 => 0
        case _ => tp.value / (tp.value + fp.value).toDouble
      }
    }

    /**
     * compute precision for multi-class classifier using weighted averaging
     */
    def multiclassPrecision = {
      val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

      val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)
      val predictGroupedRdd = pairedRdd.groupBy(pair => pair._2)

      val joinedRdd = labelGroupedRdd.join(predictGroupedRdd)

      val weightedPrecisionRdd: RDD[Double] = joinedRdd.map { label =>
        // label is tuple of (labelValue, (SeqOfInstancesWithThisActualLabel, SeqOfInstancesWithThisPredictedLabel))
        val labelCount = label._2._1.size // get the number of instances with this label as the actual label
        var correctPredict: Long = 0
        val totalPredict = label._2._2.size

        label._2._1.map { prediction =>
          if (prediction._1.equals(prediction._2)) {
            correctPredict += 1
          }
        }
        totalPredict match {
          case 0 => 0
          case _ => labelCount * (correctPredict / totalPredict.toDouble)
        }
      }
      weightedPrecisionRdd.sum() / pairedRdd.count().toDouble
    }

    // determine if this is binary or multi-class classifier
    frameRdd.map(row => row(labelColumnIndex)).distinct().count() match {
      case x if x == 1 || x == 2 => binaryPrecision
      case y if y > 2 => multiclassPrecision
      case _ => throw new IllegalArgumentException()
    }
  }

  /**
   * Compute recall of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param posLabel the label for a positive instance
   * @return a Double of the model recall measure
   */
  def modelRecall(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    /**
     * compute recall for binary classifier: TP / (TP + FN)
     */
    def binaryRecall = {
      val tp = frameRdd.sparkContext.accumulator[Long](0)
      val fn = frameRdd.sparkContext.accumulator[Long](0)

      frameRdd.foreach { row =>
        if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          tp.add(1)
        }
        else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
          fn.add(1)
        }
      }

      (tp.value + fn.value) match {
        case 0 => 0
        case _ => tp.value / (tp.value + fn.value).toDouble
      }
    }

    /**
     * compute recall for multi-class classifier using weighted averaging
     */
    def multiclassRecall = {
      val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

      val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)

      val weightedRecallRdd: RDD[Double] = labelGroupedRdd.map { label =>
        // label is tuple of (labelValue, SeqOfInstancesWithThisActualLabel)
        var correctPredict: Long = 0

        label._2.map { prediction =>
          if (prediction._1.equals(prediction._2)) {
            correctPredict += 1
          }
        }
        correctPredict
      }
      weightedRecallRdd.sum() / pairedRdd.count().toDouble
    }

    // determine if this is binary or multi-class classifier
    frameRdd.map(row => row(labelColumnIndex)).distinct().count() match {
      case x if x == 1 || x == 2 => binaryRecall
      case y if y > 2 => multiclassRecall
      case _ => throw new IllegalArgumentException()
    }
  }

  /**
   * Compute f measure of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param posLabel the label for a positive instance
   * @param beta the beta value to use to compute the f measure
   * @return a Double of the model f measure
   */
  def modelFMeasure(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String, beta: Double): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    /**
     * compute recall for binary classifier
     */
    def binaryFMeasure = {
      val tp = frameRdd.sparkContext.accumulator[Long](0)
      val fp = frameRdd.sparkContext.accumulator[Long](0)
      val fn = frameRdd.sparkContext.accumulator[Long](0)

      frameRdd.foreach { row =>
        if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          tp.add(1)
        }
        else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          fp.add(1)
        }
        else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
          fn.add(1)
        }
      }

      val precision = (tp.value + fp.value) match {
        case 0 => 0
        case _ => tp.value / (tp.value + fp.value).toDouble
      }

      val recall = (tp.value + fn.value) match {
        case 0 => 0
        case _ => tp.value / (tp.value + fn.value).toDouble
      }

      ((pow(beta, 2) * precision + recall)) match {
        case 0 => 0
        case _ => (1 + pow(beta, 2)) * ((precision * recall) / ((pow(beta, 2) * precision) + recall))
      }
    }

    /**
     * compute f measure for multi-class classifier using weighted averaging
     */
    def multiclassFMeasure = {
      val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

      val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)
      val predictGroupedRdd = pairedRdd.groupBy(pair => pair._2)

      val joinedRdd = labelGroupedRdd.join(predictGroupedRdd)

      val weightedFMeasureRdd: RDD[Double] = joinedRdd.map { label =>
        // label is tuple of (labelValue, (SeqOfInstancesWithThisActualLabel, SeqOfInstancesWithThisPredictedLabel))
        val labelCount = label._2._1.size // get the number of instances with this label as the actual label
        var correctPredict: Long = 0
        val totalPredict = label._2._2.size

        label._2._1.map { prediction =>
          if (prediction._1.equals(prediction._2)) {
            correctPredict += 1
          }
        }

        val precision = totalPredict match {
          case 0 => 0
          case _ => labelCount * (correctPredict / totalPredict.toDouble)
        }

        val recall = labelCount match {
          case 0 => 0
          case _ => labelCount * (correctPredict / labelCount.toDouble)
        }

        ((pow(beta, 2) * precision) + recall) match {
          case 0 => 0
          case _ => (1 + pow(beta, 2)) * ((precision * recall) / ((pow(beta, 2) * precision) + recall))
        }
      }
      weightedFMeasureRdd.sum() / pairedRdd.count().toDouble
    }

    // determine if this is binary or multi-class classifier
    frameRdd.map(row => row(labelColumnIndex)).distinct().count() match {
      case x if x == 1 || x == 2 => binaryFMeasure
      case y if y > 2 => multiclassFMeasure
      case _ => throw new IllegalArgumentException()
    }
  }

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

  /**
   * Generate the empirical cumulative distribution for an input dataframe column
   *
   * @param frameRdd rdd for a BigFrame
   * @param sampleIndex index of the column containing the sample data
   * @param dataType the data type of the input column
   * @return a new RDD of tuples containing each distinct sample value and its ecdf value
   */
  def ecdf(frameRdd: RDD[Row], sampleIndex: Int, dataType: String): RDD[Row] = {
    // parse values
    val pairedRdd = try {
      frameRdd.map(row => (java.lang.Double.parseDouble(row(sampleIndex).toString), java.lang.Double.parseDouble(row(sampleIndex).toString)))
    }
    catch {
      case cce: NumberFormatException => throw new NumberFormatException("Non-numeric column: " + cce.toString)
    }

    // get sample size
    val numValues = pairedRdd.count().toDouble

    // group identical values together
    val groupedRdd = pairedRdd.groupByKey()

    // count number of each distinct value and sort by distinct value
    val sortedRdd = groupedRdd.map { case (value, seqOfValue) => (value, seqOfValue.size) }.sortByKey()

    // compute the partition sums
    val partSums: Array[Double] = 0.0 +: sortedRdd.mapPartitionsWithIndex {
      case (index, partition) => Iterator(partition.map(pair => pair._2).sum.toDouble)
    }.collect()

    // compute empirical cumulative distribution
    val sumsRdd = sortedRdd.mapPartitionsWithIndex {
      case (index, partition) => {
        var startValue = 0.0
        for (i <- 0 to index) {
          startValue += partSums(i)
        }
        partition.scanLeft((0.0, startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
      }
    }

    sumsRdd.map {
      case (value, valueSum) => {
        dataType match {
          case "int32" => Array(value.toInt.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case "int64" => Array(value.toLong.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case "float32" => Array(value.toFloat.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case "float64" => Array(value.toDouble.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
          case _ => Array(value.asInstanceOf[Any], (valueSum / numValues).asInstanceOf[Any])
        }
      }
    }
  }

  def aggregation_functions(elem: Seq[Array[Any]],
                            args_pair: Seq[(Int, String)],
                            schema: List[(String, DataTypes.DataType)]): Seq[Any] = {
    for {
      i <- args_pair
    } yield (i, schema(i._1)._2) match {
      case ((j: Int, "SUM"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).sum
      case ((j: Int, "SUM"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).sum
      case ((j: Int, "SUM"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).sum
      case ((j: Int, "SUM"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).sum
      case ((j: Int, "MAX"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).max
      case ((j: Int, "MAX"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).max
      case ((j: Int, "MAX"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).max
      case ((j: Int, "MAX"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).max
      case ((j: Int, "MIN"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).min
      case ((j: Int, "MIN"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).min
      case ((j: Int, "MIN"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).min
      case ((j: Int, "MIN"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).min
      case ((j: Int, "AVG"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).sum * 1.0 / elem.length
      case ((j: Int, "AVG"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).sum * 1.0 / elem.length
      case ((j: Int, "AVG"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).sum / elem.length
      case ((j: Int, "AVG"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).sum / elem.length

      case ((j: Int, "COUNT"), _) => elem.length
      case ((j: Int, "COUNT_DISTINCT"), _) => elem.map(e => e(j)).distinct.length

      case ((j: Int, "VAR"), DataTypes.int32) =>
        val elements: Seq[Int] = elem.map(e => e(j).asInstanceOf[Int])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length
      case ((j: Int, "VAR"), DataTypes.int64) =>
        val elements: Seq[Long] = elem.map(e => e(j).asInstanceOf[Long])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length
      case ((j: Int, "VAR"), DataTypes.float32) =>
        val elements: Seq[Float] = elem.map(e => e(j).asInstanceOf[Float])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length
      case ((j: Int, "VAR"), DataTypes.float64) =>
        val elements: Seq[Double] = elem.map(e => e(j).asInstanceOf[Double])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length

      case ((j: Int, "STDEV"), DataTypes.int32) =>
        val elements: Seq[Int] = elem.map(e => e(j).asInstanceOf[Int])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)
      case ((j: Int, "STDEV"), DataTypes.int64) =>
        val elements: Seq[Long] = elem.map(e => e(j).asInstanceOf[Long])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)
      case ((j: Int, "STDEV"), DataTypes.float32) =>
        val elements: Seq[Float] = elem.map(e => e(j).asInstanceOf[Float])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)
      case ((j: Int, "STDEV"), DataTypes.float64) =>
        val elements: Seq[Double] = elem.map(e => e(j).asInstanceOf[Double])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)

      case _ => throw new IllegalArgumentException(s"Invalid aggregation function $i._2")
    }
  }

  def convertGroupBasedOnSchema(groupedColumnSchema: Array[DataTypes.DataType], data: String): Array[Any] =
    if (data != null && data != "")
      DataTypes.parseMany(groupedColumnSchema)(data.split("\0"))
    else
      Array[Any]()

  def aggregation(groupedRDD: RDD[(String, Iterable[Array[Any]])],
                  args_pair: Seq[(Int, String)],
                  schema: List[(String, DataTypes.DataType)],
                  groupedColumnSchema: Array[DataTypes.DataType],
                  arguments: FrameGroupByColumn[JsObject, Long]): FrameRDD = {

    val resultRdd = groupedRDD.map(elem =>
      convertGroupBasedOnSchema(groupedColumnSchema, elem._1) ++ aggregation_functions(elem._2.toSeq, args_pair, schema))

    val aggregated_column_schema = for {
      i <- args_pair
    } yield {
      i._2 match {
        case "COUNT" | "COUNT_DISTINCT" => DataTypes.int64
        case "SUM" | "MIN" | "MAX" => schema(i._1)._2
        case _ => DataTypes.float64
      }
    }

    val new_data_types = groupedColumnSchema ++ aggregated_column_schema

    val new_column_names = arguments.group_by_columns ++ {
      for { i <- arguments.aggregations } yield i._3
    }
    val new_schema = new_column_names.zip(new_data_types)

    new FrameRDD(new Schema(new_schema), resultRdd)
  }

  /**
   * Remove duplicate rows identified by the key
   * @param pairRdd rdd which has (key, value) structure in each row
   */
  def removeDuplicatesByKey(pairRdd: RDD[(Seq[Any], Array[Any])]): RDD[Array[Any]] = {
    val grouped = pairRdd.groupByKey()
    val duplicatesRemoved: RDD[Array[Any]] = grouped.map(bag => {
      val firstEntry = bag._2.head
      firstEntry
    })
    duplicatesRemoved
  }

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
  def quantiles(rdd: RDD[Row], quantiles: Seq[Int], columnIndex: Int, dataType: DataType): Seq[Quantile] = {
    val totalRows = rdd.count()
    val pairRdd = rdd.map(row => SparkOps.createKeyValuePairFromRow(row, List(columnIndex))).map { case (keyColumns, data) => (keyColumns(0).toString.toDouble, data) }
    val sorted = pairRdd.asInstanceOf[RDD[(Double, Row)]].sortByKey(true)

    val quantileTargetMapping = getQuantileTargetMapping(totalRows, quantiles)
    val sumsAndCounts: Map[Int, (Int, Int)] = getPerPartitionCountAndAccumulatedSum(sorted)

    //this is the first stage of calculating quantile
    //generate data that has keys as quantiles and values as column data times weight
    val quantilesComponentsRDD = sorted.mapPartitionsWithIndex((partitionIndex, rows) => {
      var rowIndex: Long = (if (partitionIndex == 0) 0 else sumsAndCounts(partitionIndex - 1)._2) + 1
      val perPartitionResult = ListBuffer[(Int, BigDecimal)]()

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
   * For whole quantile calculation process, please refer to doc of method calculatePercentiles
   * @param totalRows
   * @param quantile
   */
  def getQuantileComposingElements(totalRows: Long, quantile: Int): Seq[QuantileComposingElement] = {
    val position = (BigDecimal(quantile) * totalRows) / 100
    var integerPosition = position.toLong
    val fractionPosition = position - integerPosition

    val result = mutable.ListBuffer[QuantileComposingElement]()

    val addQuantileComposingElement = (position: Long, quantile: Int, weight: BigDecimal) => {
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
   * For whole quantile calculation process, please refer to doc of method calculatePercentiles
   */
  def getQuantileTargetMapping(totalRows: Long, quantiles: Seq[Int]): Map[Long, Seq[QuantileTarget]] = {

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
  def confusionMatrix(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String): Seq[Long] = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val tp = frameRdd.sparkContext.accumulator[Long](0)
    val tn = frameRdd.sparkContext.accumulator[Long](0)
    val fp = frameRdd.sparkContext.accumulator[Long](0)
    val fn = frameRdd.sparkContext.accumulator[Long](0)

    frameRdd.foreach { row =>
      if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
        tp.add(1)
      }
      else if (!row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
        tn.add(1)
      }
      else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
        fp.add(1)
      }
      else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
        fn.add(1)
      }
    }

    val labels = frameRdd.map(row => row(labelColumnIndex)).distinct().collect()
    labels.size match {
      case x if x == 1 || x == 2 => Seq(tp.value, tn.value, fp.value, fn.value)
      case _ => throw new IllegalArgumentException("Confusion matrix only supports binary classifiers")
    }
  }

}
