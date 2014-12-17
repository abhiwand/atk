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

import com.intel.intelanalytics.domain.{ BoolValue, LongValue }
import com.intel.intelanalytics.engine.spark.frame.{ MiscFrameFunctions, FrameRDD }

/**
 * Object for exporting frames to files
 */

object ExportHDFSFunctions extends Serializable {

  /**
   * Export to a file in CSV format
   *
   * @param frameRDD input rdd containing all columns
   * @param filename file path where to store the file
   * @return Long true or false
   */
  def exportToCsvHdfs(
    frameRDD: FrameRDD,
    filename: String,
    count: Int,
    offset: Int,
    separator: String): BoolValue = {

    val filterRdd = if (count != -1) MiscFrameFunctions.getPagedRdd(frameRDD, offset, count, -1) else frameRDD
    val headers = frameRDD.frameSchema.columnNamesAsString
    val csvRdd = filterRdd.map(x => {
      val b = x.map(a => a.toString).mkString(separator)
      b
    })
    val addHeaders = frameRDD.sparkContext.parallelize(List(headers)) ++ csvRdd
    addHeaders.saveAsTextFile(filename)
    BoolValue(true)
  }

}

