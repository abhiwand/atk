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
import com.intel.intelanalytics.domain.LoadLines
import spray.json.JsObject

/**
 * This object exists to avoid having to serialize the entire engine in order to use spark
 */
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
}
