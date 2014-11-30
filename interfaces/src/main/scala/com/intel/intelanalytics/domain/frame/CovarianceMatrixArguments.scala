package com.intel.intelanalytics.domain.frame

case class CovarianceMatrixArguments(frame: FrameReference,
                                     dataColumnNames: List[String],
                                     matrixName: Option[String]) {
  require(frame != null, "frame is required")
  require(dataColumnNames.size >= 2, "two or more data columns are required")
}