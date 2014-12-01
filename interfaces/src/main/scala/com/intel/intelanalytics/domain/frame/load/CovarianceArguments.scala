package com.intel.intelanalytics.domain.frame

case class CovarianceArguments(frame: FrameReference,
                               dataColumnNames: List[String]) {
  require(frame != null, "frame is required")
  require(dataColumnNames.size == 2, "exactly two data columns are required")
}

case class CovarianceReturn(covariance: Double)