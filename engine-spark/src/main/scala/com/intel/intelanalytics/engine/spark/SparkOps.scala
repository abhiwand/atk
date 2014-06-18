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
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import spray.json.JsObject
import scala.collection.mutable
import scala.Some
import com.intel.intelanalytics.engine.spark.frame.RDDJoinParam
import com.intel.intelanalytics.domain.frame.LoadLines
import com.intel.intelanalytics.domain.frame.LoadLines


private[spark] object SparkOps extends Serializable {

  def getRows(rdd: RDD[Row], offset: Long, count: Int, limit: Int): Seq[Row] = {

    //Count the rows in each partition, then order the counts by partition number
    val counts = rdd.mapPartitionsWithIndex(
      (i: Int, rows: Iterator[Row]) => Iterator.single((i, rows.size)))
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


  def loadLines(ctx: SparkContext,
                fileName: String,
                location: String,
                arguments: LoadLines[JsObject, Long],
                parserFunction: String => Array[String],
                converter: Array[String] => Array[Any]) = {
    ctx.textFile(fileName)
      .mapPartitionsWithIndex {
        case (partition, lines) => {
          if (partition == 0) {
            lines.drop(arguments.skipRows.getOrElse(0)).map(parserFunction)
          }
          else {
            lines.map(parserFunction)
          }
        }
      }
      .map(converter)
      .saveAsObjectFile(location)
  }

  /**
   * generate 2 tuple instance in order to invoke pairRDD functions
   * @param data row data
   * @param keyIndex index of the key column
   */
  def createKeyValuePairFromRow(data: Array[Any], keyIndex: Seq[Int]): (Seq[Any], Array[Any]) = {

    var key: Seq[Any] = Seq()
    for(i <- keyIndex)
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
                  location: String) : Seq[DataTypes.DataType] = {

    rdd.map(elem =>
      convertGroupBasedOnSchema(groupedColumnSchema, elem._1) ++ aggregation_functions(elem._2, args_pair, schema))
      .saveAsObjectFile(location)

    val aggregated_column_schema = for {
      i <- args_pair
    } yield {
      i._2 match {
        case "COUNT" | "COUNT_DISTINCT"  => DataTypes.int64
        case "SUM" | "MIN" | "MAX" => schema(i._1)._2
        case _ => DataTypes.float64
      }
    }
    groupedColumnSchema ++ aggregated_column_schema
  }

  def removeDuplicatesByKey(pairRdd: RDD[(Seq[Any], Array[Any])]): RDD[Array[Any]] = {
    import org.apache.spark.SparkContext._
    val grouped = pairRdd.groupByKey()
    val duplicatesRemoved = grouped.map(bag => {
      val firstEntry = bag._2(0)
      firstEntry
    })
    duplicatesRemoved
  }

}
