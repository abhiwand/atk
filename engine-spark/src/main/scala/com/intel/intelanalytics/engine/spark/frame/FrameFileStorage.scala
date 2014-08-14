package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.engine.{ File, Directory, FileStorage }
import scala.util.matching.Regex
import java.nio.file.{ Path, Paths }
import com.intel.intelanalytics.domain.frame.DataFrame
import java.util.ConcurrentModificationException
import org.apache.commons.lang3.concurrent.ConcurrentRuntimeException
import com.intel.intelanalytics.shared.EventLogging
import java.net.URI

/**
 * Frame storage in HDFS.
 *
 * Each frame revision is stored into a new location so that we don't get problems
 * with reading and writing to the same location at the same time.
 *
 * @param fsRoot root for our application, e.g. "hdfs://hostname/user/iauser"
 * @param fileStorage methods for interacting with underlying storage (e.g. HDFS)
 */
class FrameFileStorage(fsRoot: String,
                       fileStorage: FileStorage) extends EventLogging {

  private val framesBaseDirectory = new URI(fsRoot + "/intelanalytics/dataframes")

  withContext("FrameFileStorage") {
    info("fsRoot: " + fsRoot)
    info("data frames base directory: " + framesBaseDirectory)
  }

  /** Current revision for a frame */
  def currentFrameRevision(dataFrame: DataFrame): URI = {
    require(dataFrame.revision > 0, "Revision should be larger than zero")
    frameRevisionDirectory(dataFrame.id, dataFrame.revision)
  }

  /**
   * Create the Next revision for a frame.
   */
  def createFrameRevision(dataFrame: DataFrame, revision: Int): URI = withContext("createFrameRevision") {
    require(revision > 0, "Revision should be larger than zero")
    require(revision > dataFrame.revision, s"New revision should be larger than the old revision: $dataFrame $revision")

    val path = frameRevisionDirectory(dataFrame.id, revision)
    if (fileStorage.exists(path)) {
      error("Next frame revision already exists " + path
        + " You may be attempting to modify a data frame that is already in the process of being modified")

      // TODO: It would be nice to throw an Exception here but we probably need to handle locking first
      fileStorage.delete(path)
    }
    fileStorage.createDirectory(path)
    path
  }

  /**
   * Remove the directory and underlying data for a particular revision of a data frame
   * @param dataFrame the data frame to act on
   * @param revision the revision to remove
   */
  def deleteFrameRevision(dataFrame: DataFrame, revision: Int): Unit = {
    if (revision > 0) {
      fileStorage.delete(frameRevisionDirectory(dataFrame.id, revision))
    }
  }

  /**
   * Remove the underlying data file from HDFS - remove any revision that exists
   *
   * @param dataFrame the frame to completely remove
   */
  def delete(dataFrame: DataFrame): Unit = {
    fileStorage.delete(frameBaseDirectory(dataFrame.id))
  }

  /** Base dir for a particular revision of a frame */
  private[frame] def frameRevisionDirectory(frameId: Long, revision: Int): URI = {
    new URI(frameBaseDirectory(frameId) + "/rev" + revision)
  }

  /** Base dir for a frame */
  private[frame] def frameBaseDirectory(frameId: Long): URI = {
    new URI(framesBaseDirectory + "/" + frameId)
  }

}
