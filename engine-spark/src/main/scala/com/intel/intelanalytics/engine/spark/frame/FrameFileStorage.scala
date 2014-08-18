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

import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.engine.spark.HdfsFileStorage
import org.apache.hadoop.fs.Path

/**
 * Frame storage in HDFS.
 *
 * Each frame revision is stored into a new location so that we don't get problems
 * with reading and writing to the same location at the same time.
 *
 * @param fsRoot root for our application, e.g. "hdfs://hostname/user/iauser"
 * @param hdfs methods for interacting with underlying storage (e.g. HDFS)
 */
class FrameFileStorage(fsRoot: String,
                       hdfs: HdfsFileStorage) extends EventLogging {

  private val framesBaseDirectory = new Path(fsRoot + "/intelanalytics/dataframes")

  withContext("FrameFileStorage") {
    info("fsRoot: " + fsRoot)
    info("data frames base directory: " + framesBaseDirectory)
  }

  /** Current revision for a frame */
  def currentFrameRevision(dataFrame: DataFrame): Path = {
    require(dataFrame.revision > 0, "Revision should be larger than zero")
    frameRevisionDirectory(dataFrame.id, dataFrame.revision)
  }

  /**
   * Create the Next revision for a frame.
   */
  def createFrameRevision(dataFrame: DataFrame, revision: Int): Path = withContext("createFrameRevision") {
    require(revision > 0, "Revision should be larger than zero")
    require(revision > dataFrame.revision, s"New revision should be larger than the old revision: $dataFrame $revision")

    val path = frameRevisionDirectory(dataFrame.id, revision)
    if (hdfs.exists(path)) {
      error("Next frame revision already exists " + path
        + " You may be attempting to modify a data frame that is already in the process of being modified")

      // TODO: It would be nice to throw an Exception here but we probably need to handle locking first
      hdfs.delete(path)
    }
    hdfs.createDirectory(path)
    path
  }

  /**
   * Remove the directory and underlying data for a particular revision of a data frame
   * @param dataFrame the data frame to act on
   * @param revision the revision to remove
   */
  def deleteFrameRevision(dataFrame: DataFrame, revision: Int): Unit = {
    if (revision > 0) {
      hdfs.delete(frameRevisionDirectory(dataFrame.id, revision), recursive = true)
    }
  }

  /**
   * Remove the underlying data file from HDFS - remove any revision that exists
   *
   * @param dataFrame the frame to completely remove
   */
  def delete(dataFrame: DataFrame): Unit = {
    hdfs.delete(frameBaseDirectory(dataFrame.id), recursive = true)
  }

  /** Base dir for a particular revision of a frame */
  private[frame] def frameRevisionDirectory(frameId: Long, revision: Int): Path = {
    new Path(frameBaseDirectory(frameId) + "/rev" + revision)
  }

  /** Base dir for a frame */
  private[frame] def frameBaseDirectory(frameId: Long): Path = {
    new Path(framesBaseDirectory + "/" + frameId)
  }

}
