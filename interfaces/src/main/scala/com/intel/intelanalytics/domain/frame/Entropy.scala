package com.intel.intelanalytics.domain.frame

/**
 * Command for calculating the Shannon entropy of a column in a data frame.
 *
 * @param dataColumn Name of the column to compute entropy.
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 *
 */
case class Entropy(frameRef: FrameReference, dataColumn: String, weightsColumn: Option[String] = None) {
  require(frameRef != null, "frame is required")
  require(dataColumn != null, "column name is required")
}

/**
 * Return value for Shannon entropy command.
 *
 * @param entropy Shannon entropy
 */
case class EntropyReturn(entropy: Double)