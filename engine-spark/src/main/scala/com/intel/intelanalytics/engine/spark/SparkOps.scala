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

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.Some
import com.intel.intelanalytics.engine.spark.frame.RDDJoinParam
import com.intel.intelanalytics.algorithm.{ PercentileTarget, PercentileElement }
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.schema.DataTypes.DataType

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

private[spark] object SparkOps extends Serializable {

  def getRows(rdd: RDD[Row], offset: Long, count: Int, limit: Int): Seq[Row] = {

    val sumsAndCounts: Map[Int, (Int, Int)] = getPerPartitionCountAndAccumulatedSum(rdd)

    //println(sumsAndCounts)
    val capped = Math.min(count, limit)

    //Start getting rows. We use the sums and counts to figure out which
    //partitions we need to read from and which to just ignore
    val rows: Seq[Row] = rdd.mapPartitionsWithIndex((i, rows) => {
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
    }).collect()
    rows
  }

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

      case _ => left.rdd.join(right.rdd).map(t => {
        val leftValues: Array[Any] = t._2._1
        val rightValues: mutable.ArrayOps[Any] = t._2._2
        leftValues ++ rightValues
      })
    }

    result.asInstanceOf[RDD[Array[Any]]]
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
   * Bin column at index using equal width binning.
   *
   * This uses the Spark histogram function to get the cutoffs, then map each input rdd column value to a bin number
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
          if ((i == cutoffs.length - 2) && (element - cutoffs(i) >= 0)) { // && (element - cutoffs(i + 1) <= 0)) {
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

  def aggregation(rdd: RDD[(String, Seq[Array[Any]])],
                  args_pair: Seq[(Int, String)],
                  schema: List[(String, DataTypes.DataType)],
                  groupedColumnSchema: Array[DataTypes.DataType],
                  location: String): Seq[DataTypes.DataType] = {

    rdd.map(elem =>
      convertGroupBasedOnSchema(groupedColumnSchema, elem._1) ++ aggregation_functions(elem._2, args_pair, schema))
      .saveAsObjectFile(location)

    val aggregated_column_schema = for {
      i <- args_pair
    } yield {
      i._2 match {
        case "COUNT" | "COUNT_DISTINCT" => DataTypes.int64
        case "SUM" | "MIN" | "MAX" => schema(i._1)._2
        case _ => DataTypes.float64
      }
    }
    groupedColumnSchema ++ aggregated_column_schema
  }

  /**
   * Remove duplicate rows identified by the key
   * @param pairRdd rdd which has (key, value) structure in each row
   */
  def removeDuplicatesByKey(pairRdd: RDD[(Seq[Any], Array[Any])]): RDD[Array[Any]] = {
    val grouped = pairRdd.groupByKey()
    val duplicatesRemoved = grouped.map(bag => {
      val firstEntry = bag._2(0)
      firstEntry
    })
    duplicatesRemoved
  }

  /**
   * calculate and return elements for calculating percentile
   * For examle, 25th percentile out of 10 rows(x1, x2, x3, ... x10) will be
   * 0.5 * x2 + 0.5 * x3. The method will return x2 and x3 with weight as 0.5
   * @param totalRows
   * @param percentile
   */
  def getPercentileComposingElements(totalRows: Long, percentile: Int): Seq[PercentileElement] = {
    val position = percentile.toDouble * totalRows.toDouble / 100
    var integer = position.toInt
    val decimal = BigDecimal(position - integer).setScale(2, BigDecimal.RoundingMode.DOWN).toFloat

    val result = mutable.ListBuffer[PercentileElement]()

    //element starts from 1. therefore x0 equals x1
    if (integer == 0)
      integer = 1

    if ((1 - decimal) > 0)
      result += PercentileElement(integer, 1 - decimal)

    if (decimal > 0)
      result += PercentileElement(integer + 1, decimal)

    result.toSeq
  }

  /**
   * Calculate mapping between an element's position and Seq of percentile that the element can contribute to
   * @param totalRows total number of rows in the data
   * @param percentiles Sequence of percentiles to search
   */
  def getPercentileTargetMapping(totalRows: Long, percentiles: Seq[Int]): Map[Long, Seq[PercentileTarget]] = {

    val mapping = percentiles.flatMap(percentile => getPercentileComposingElements(totalRows, percentile).map(element => {
      val elementIndex: Int = element.index
      (elementIndex, PercentileTarget(percentile, element.weight))
    })).
      foldLeft(mutable.Map[Long, ListBuffer[PercentileTarget]]())((mapping, element) => {
        val elementPosition: Long = element._1
        if (!mapping.contains(elementPosition))
          mapping(elementPosition) = ListBuffer[PercentileTarget]()

        mapping(elementPosition) += element._2
        mapping
      })

    mapping.map(i => (i._1, i._2.toSeq)).toMap
  }

  def calculatePercentiles(rdd: RDD[Row], percentiles: Seq[Int], columnIndex: Int, dataType: DataType): Seq[Any] = {
    val totalRows = rdd.count()
    val pairRdd = rdd.map(row => SparkOps.createKeyValuePairFromRow(row, List(columnIndex))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val sorted = pairRdd.asInstanceOf[RDD[(Int, Row)]].sortByKey(true)

    val percentileTargetMapping = getPercentileTargetMapping(totalRows, percentiles)
    val sumsAndCounts: Map[Int, (Int, Int)] = getPerPartitionCountAndAccumulatedSum(sorted)

    //this is the first stage of calculating percentile
    //generate data that has keys as percentiles and values as column data times weight
    val percentilesComponentsRDD = sorted.mapPartitionsWithIndex((partitionIndex, rows) => {
      var rowIndex: Long = (if (partitionIndex == 0) 0 else sumsAndCounts(partitionIndex - 1)._2) + 1
      val result = ListBuffer[(Int, BigDecimal)]()

      for (row <- rows) {
        if (percentileTargetMapping.contains(rowIndex)) {
          val target: Seq[PercentileTarget] = percentileTargetMapping(rowIndex)

          for (percentile <- target) {
            val value = row._1
            val numericVal = DataTypes.toBigDecimal(value)
            result += Tuple2(percentile.percentile, numericVal * percentile.weight)
          }
        }

        rowIndex = rowIndex + 1
      }

      result.toIterator
    })

    percentilesComponentsRDD.reduceByKey(_ + _).sortByKey(true).collect()
  }

}
