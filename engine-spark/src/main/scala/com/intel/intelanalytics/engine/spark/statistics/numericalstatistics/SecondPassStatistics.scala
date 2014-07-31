package com.intel.intelanalytics.engine.spark.statistics.numericalstatistics

import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulatorParam
import com.intel.intelanalytics.engine.spark.statistics.NumericValidationUtils

/**
 * Second pass statistics - for computing higher moments about the mean.
 *
 * xw : the weighted mean of the data
 * sw: the weighted standard deviation of the data
 *
 * @param sumOfThirdWeighted  Sum over data-weight pairs (x, w) of  Math.pow(w, 1.5) * Math.pow((x - xw) / sw, 3).
 *                            Used to calculate skewness.
 * @param sumOfFourthWeighted Sum over data-weight pairs (x, w) of Math.pow(w, 2) * Math.pow(((x - xw) / sw), 4) })
 *                            Used to calculate kurtosis.
 */
private[numericalstatistics] case class SecondPassStatistics(sumOfThirdWeighted: Option[BigDecimal],
                                                             sumOfFourthWeighted: Option[BigDecimal]) extends Serializable

