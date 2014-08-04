package com.intel.intelanalytics.domain.frame

/**
 * Command for calculating the empirical entropy of a column in a data frame.
 *
 * @param frame Reference to the input data frame
 * @param columnName Column name
 */
case class Entropy(frame: FrameReference, columnName: String) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
}

/**
 * Return value for entropy command.
 *
 * @param entropy Empirical entropy
 */
case class EntropyReturn(entropy: Double)