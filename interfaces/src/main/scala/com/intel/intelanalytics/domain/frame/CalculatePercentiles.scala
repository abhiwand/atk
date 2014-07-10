package com.intel.intelanalytics.domain.frame

/**
 * Command for calculating percentiles values
 * @param frameId id of the data frame
 * @param percentiles the percentiles to calculate value for
 */
case class CalculatePercentiles(frameId: Long, percentiles: List[Int])
