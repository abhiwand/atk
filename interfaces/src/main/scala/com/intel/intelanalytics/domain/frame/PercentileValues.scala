package com.intel.intelanalytics.domain.frame

/**
 * The result object for percentile calculation
 * @param percentiles value for the percentiles
 */
case class PercentileValues(percentiles: List[(Int, BigDecimal)])
