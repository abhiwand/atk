package com.intel.intelanalytics.algorithm

/**
 * percentile composing element which contains element's index and its weight
 * @param index element index
 * @param percentileTarget the percentile target that the element can be applied to
 */
case class PercentileComposingElement(index: Long, percentileTarget: PercentileTarget)
