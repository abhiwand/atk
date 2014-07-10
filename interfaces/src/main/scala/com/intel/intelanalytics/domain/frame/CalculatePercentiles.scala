package com.intel.intelanalytics.domain.frame

/**
 * Command for calculating percentiles values
 * @param frameId id of the data frame
 * @param percentiles the percentiles to calculate value for
 * @param columnName name of the column to find percentiles
 */
case class CalculatePercentiles(frameId: Long, percentiles: List[Int], columnName: String)
