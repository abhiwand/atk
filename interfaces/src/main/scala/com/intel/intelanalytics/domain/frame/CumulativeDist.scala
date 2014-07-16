package com.intel.intelanalytics.domain.frame

case class CumulativeDist[FrameRef](frameId: FrameRef, name: String, sampleCol: String, distType: String, countValue: String) {
  require(name != null, "new frame name is required")
  require(frameId != null, "frame is required")
  require(sampleCol != null, "column name for sample is required")
  require(distType.equals("cumulative_sum") ||
    distType.equals("cumulative_count") ||
    distType.equals("cumulative_percent_sum") ||
    distType.equals("cumulative_percent_count"), "invalid distribution type")
}
