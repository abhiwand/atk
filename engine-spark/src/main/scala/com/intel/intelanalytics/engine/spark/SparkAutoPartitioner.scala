package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.FileStorage
import com.intel.intelanalytics.shared.EventLogging

/**
 * Calculate a best guess for the number of partitions that should be used for loading this file into a Spark RDD.
 *
 * This number won't be perfect but should be better than using default.
 */
class SparkAutoPartitioner(fileStorage: FileStorage) extends EventLogging {

  /**
   * Calculate a best guess for the number of partitions that should be used for loading this file into a Spark RDD.
   *
   * This number won't be perfect but should be better than using default.
   *
   * @param path relative path
   * @return number of partitions that should be used for loading this file into a Spark RDD
   */
  def partitionsForFile(path: String): Int = withContext[Int]("spark-auto-partioning") {
    val size = fileStorage.size(path)
    val partitions = partitionsFromFileSize(size)
    info("auto partitioning path:" + path + ", size:" + size + ", partitions:" + partitions)
    partitions
  }

  /**
   * Get the partition count given a file size,
   * @param fileSize size of file in bytes
   * @return partition count that should be used
   */
  private[spark] def partitionsFromFileSize(fileSize: Long): Int = {
    var partitionCount = SparkEngineConfig.maxPartitions
    SparkEngineConfig.autoPartitionerConfig.foreach(partitionConfig => {
      if (fileSize <= partitionConfig.fileSizeUpperBound) {
        partitionCount = partitionConfig.partitionCount
      }
      else {
        // list is sorted, so we can exit early
        return partitionCount
      }
    })
    partitionCount
  }
}

/**
 * Map upper bounds of file size to partition sizes
 * @param fileSizeUpperBound upper bound on file size for the partitionCount in bytes
 * @param partitionCount number of partitions to use
 */
case class FileSizeToPartitionSize(fileSizeUpperBound: Long, partitionCount: Int)