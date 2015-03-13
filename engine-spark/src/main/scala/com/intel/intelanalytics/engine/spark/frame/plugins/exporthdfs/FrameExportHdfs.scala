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

package com.intel.intelanalytics.engine.spark.frame.plugins.exporthdfs

import com.intel.intelanalytics.engine.spark.frame.MiscFrameFunctions
import org.apache.spark.frame.FrameRdd
import org.apache.commons.csv.{ CSVPrinter, CSVFormat }

import scala.collection.mutable.ArrayBuffer

/**
 * Object for exporting frames to files
 */

object FrameExportHdfs extends Serializable {

  /**
   * Export to a file in CSV format
   *
   * @param frameRdd input rdd containing all columns
   * @param filename file path where to store the file
   * @return Long true or false
   */
  def exportToHdfsCsv(
    frameRdd: FrameRdd,
    filename: String,
    separator: String,
    count: Option[Int] = None,
    offset: Option[Int] = None) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRdd, recOffset, recCount, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames.mkString(separator)
    val csvFormat = CSVFormat.RFC4180.withDelimiter(separator.trim().charAt(0))

    val csvRdd = filterRdd.map(row => {
      val stringBuilder = new java.lang.StringBuilder
      val printer = new CSVPrinter(stringBuilder, csvFormat)
      val array = row.map(col => if (col == null) "" else {
        if (col.isInstanceOf[ArrayBuffer[Double]]) {
          col.asInstanceOf[ArrayBuffer[Double]].mkString(",")
        }
        else {
          col.toString
        }
      })
      for (i <- array) printer.print(i)
      stringBuilder.toString()
    })

    val addHeaders = frameRdd.sparkContext.parallelize(List(headers)) ++ csvRdd
    addHeaders.saveAsTextFile(filename)
  }

  /**
   * Export to a file in JSON format
   *
   * @param frameRdd input rdd containing all columns
   * @param filename file path where to store the file
   * @return Long true or false
   */
  def exportToHdfsJson(
    frameRdd: FrameRdd,
    filename: String,
    count: Option[Int],
    offset: Option[Int]) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRdd, recOffset, recCount, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames
    val jsonRDD = filterRdd.map {
      row =>
        {
          val value = row.zip(headers).map {
            case (k, v) => new String("\"" + v.toString + "\":" + (if (k == null) "null"
            else if (k.isInstanceOf[String]) { "\"" + k.toString + "\"" }
            else if (k.isInstanceOf[ArrayBuffer[Double]]) { k.asInstanceOf[ArrayBuffer[Double]].mkString("[", ",", "]") }
            else k.toString)
            )
          }
          value.mkString("{", ",", "}")
        }
    }
    jsonRDD.saveAsTextFile(filename)
  }
}
