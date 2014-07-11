package com.intel.intelanalytics.domain.frame

import com.intel.intelanalytics.algorithm.Percentile

/**
 * The result object for percentile calculation
 * @param percentiles value for the percentiles
 */
case class PercentileValues(percentiles: List[Percentile])
