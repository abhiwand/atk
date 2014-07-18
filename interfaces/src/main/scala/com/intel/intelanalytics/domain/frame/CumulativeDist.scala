package com.intel.intelanalytics.domain.frame

/**
 * Represents a CumulativeDist object
 *
 * @param frameId identifier for the input data frame
 * @param name name for the new data frame with cumulative distribution column
 * @param sampleCol name of the column from which to compute a cumulative distribution
 * @param distType the desired type of cumulative distribution
 * @param dataType the data type of the input data frame
 * @param countValue the value to count in the input data frame column (only used for cumulative count and cumulative percent count)
 * @tparam FrameRef
 */
case class CumulativeDist[FrameRef](frameId: FrameRef, name: String, sampleCol: String, distType: String, dataType: String, countValue: String) {
  require(name != null, "new frame name is required")
  require(frameId != null, "frame is required")
  require(sampleCol != null, "column name for sample is required")
  require(distType.equals("cumulative_sum") ||
    distType.equals("cumulative_count") ||
    distType.equals("cumulative_percent_sum") ||
    distType.equals("cumulative_percent_count"), "invalid distribution type")
}
