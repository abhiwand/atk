package com.intel.intelanalytics.algorithm

/**
 * percentile target that will be applied to an element
 * @param percentile percentile. For eg, 40 means 40th percentile
 * @param weight weight that will be applied to the element
 */
case class PercentileTarget(percentile: Int, weight: Float)