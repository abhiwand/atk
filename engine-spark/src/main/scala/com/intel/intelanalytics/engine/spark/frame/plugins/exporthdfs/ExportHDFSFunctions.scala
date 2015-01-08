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

package com.intel.intelanalytics.engine.spark.frame.plugins.exporthdfs

import com.intel.intelanalytics.engine.spark.frame.{ MiscFrameFunctions, FrameRDD }

/**
 * Object for exporting frames to files
 */

object FrameExportHdfs extends Serializable {

  /**
   * Export to a file in CSV format
   *
   * @param frameRDD input rdd containing all columns
   * @param filename file path where to store the file
   * @return Long true or false
   */
  def exportToHdfsCsv(
    frameRDD: FrameRDD,
    filename: String,
    separator: String,
    count: Option[Int] = None,
    offset: Option[Int] = None) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRDD, recOffset, recCount, -1) else frameRDD
    val headers = frameRDD.frameSchema.columnNames.mkString(separator)
    val csvRdd = filterRdd.map(row => { row.map(col => col.toString).mkString(separator) })
    val addHeaders = frameRDD.sparkContext.parallelize(List(headers)) ++ csvRdd
    addHeaders.saveAsTextFile(filename)
  }

  /**
   * Export to a file in JSON format
   *
   * @param frameRDD input rdd containing all columns
   * @param filename file path where to store the file
   * @return Long true or false
   */
  def exportToHdfsJson(
    frameRDD: FrameRDD,
    filename: String,
    count: Option[Int],
    offset: Option[Int]) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRDD, recOffset, recCount, -1) else frameRDD
    val headers = frameRDD.frameSchema.columnNames
    val jsonRDD = filterRdd.map {
      row =>
        {
          val value = row.zip(headers).map { case (k, v) => new String("\"" + v.toString + "\":" + k.toString) }
          value.mkString("{", ",", "}")
        }
    }
    jsonRDD.saveAsTextFile(filename)
  }
}
