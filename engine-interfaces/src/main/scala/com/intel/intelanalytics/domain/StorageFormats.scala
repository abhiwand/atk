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

package com.intel.intelanalytics.domain

/**
 * Storage formats supported by the system
 */
object StorageFormats {

  val CassandraTitan = "cassandra/titan"
  val HBaseTitan = "hbase/titan"
  val SeamlessGraph = "ia/frame"
  val FileParquet = "file/parquet"
  val FileSequence = "file/sequence"

  private val graphFormats = Set(SeamlessGraph, CassandraTitan, HBaseTitan)
  private val frameFormats = Set(FileSequence, FileParquet)

  def validateGraphFormat(format: String): Unit = {
    if (!graphFormats.contains(format)) {
      throw new IllegalArgumentException(s"Unsupported graph storage format $format, please choose from " + graphFormats.mkString(", "))
    }
  }

  def validateFrameFormat(format: String): Unit = {
    if (!frameFormats.contains(format)) {
      throw new IllegalArgumentException(s"Unsupported frame storage format $format")
    }
  }

  def isSeamlessGraph(format: String): Boolean = {
    SeamlessGraph == format
  }

}
