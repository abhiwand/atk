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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.HdfsFileStorage
import org.apache.hadoop.fs.Path
import com.intel.event.{ EventContext, EventLogging }

/**
 * Frame storage in HDFS.
 *
 * @param fsRoot root for our application, e.g. "hdfs://hostname/user/iauser"
 * @param hdfs methods for interacting with underlying storage (e.g. HDFS)
 */
class FrameFileStorage(fsRoot: String,
                       val hdfs: HdfsFileStorage)(implicit startupInvocation: Invocation)
    extends EventLogging with EventLoggingImplicits {

  private val framesBaseDirectory = new Path(fsRoot + "/intelanalytics/dataframes")

  withContext("FrameFileStorage") {
    info("fsRoot: " + fsRoot)
    info("data frames base directory: " + framesBaseDirectory)
  }

  def frameBaseDirectoryExists(dataFrame: FrameEntity) = {
    val path = frameBaseDirectory(dataFrame.id)
    hdfs.exists(path)
  }

  def createFrame(dataFrame: FrameEntity): Path = withContext("createFrame") {

    if (frameBaseDirectoryExists(dataFrame)) {
      throw new IllegalArgumentException(s"Frame already exists at ${frameBaseDirectory(dataFrame.id)}")
    }
    //TODO: actually create the file?
    frameBaseDirectory(dataFrame.id)
  }

  /**
   * Remove the directory and underlying data for a particular revision of a data frame
   * @param dataFrame the data frame to act on
   */
  def delete(dataFrame: FrameEntity): Unit = {
    hdfs.delete(frameBaseDirectory(dataFrame.id), recursive = true)
  }

  /** Base dir for a frame */
  private[frame] def frameBaseDirectory(frameId: Long): Path = {
    new Path(framesBaseDirectory + "/" + frameId)
  }

  /**
   * Determine if a dataFrame is saved as parquet
   * @param dataFrame the data frame to verify
   * @return true if the data frame is saved in the parquet format
   */
  private[frame] def isParquet(dataFrame: FrameEntity): Boolean = {
    val path = frameBaseDirectory(dataFrame.id)
    hdfs.globList(path, "*.parquet").length > 0
  }

}
