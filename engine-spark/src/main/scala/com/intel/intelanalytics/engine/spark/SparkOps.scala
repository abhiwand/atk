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

import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.intel.intelanalytics.domain.LoadLines
import spray.json.JsObject
import scala.collection.mutable
import scala.Some
import com.intel.intelanalytics.engine.spark.RDDJoinParam
import com.intel.intelanalytics.domain.LoadLines

/**
 * This object exists to avoid having to serialize the entire engine in order to use spark
 */

case class RDDJoinParam(rdd: RDD[(Any, Array[Any])], columnCount: Int)

private[spark] object SparkOps extends Serializable {

  def getRows(rdd: RDD[Row], offset: Long, count: Int): Seq[Row] = {
    val counts = rdd.mapPartitionsWithIndex(
      (i: Int, rows: Iterator[Row]) => Iterator.single((i, rows.size)))
      .collect()
      .sortBy(_._1)
    val sums = counts.scanLeft((0, 0)) {
      (t1, t2) => (t2._1, t1._2 + t2._2)
    }
      .drop(1)
      .toMap
    val sumsAndCounts = counts.map {
      case (part, count) => (part, (count, sums(part)))
    }.toMap
    val rows: Seq[Row] = rdd.mapPartitionsWithIndex((i, rows) => {
      val (ct: Int, sum: Int) = sumsAndCounts(i)
      if (sum < offset || sum - ct > offset + count) {
        Iterator.empty
      }
      else {
        val start = offset - (sum - ct)
        rows.drop(start.toInt).take(count)
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
  def create2TupleForJoin(data: Array[Any], keyIndex: Int): (Any, Array[Any]) = {
    (data(keyIndex), data)
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
}
