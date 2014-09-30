package com.intel.intelanalytics.domain.frame

/**
 * Command for retrieving the top (or bottom) K distinct values by count for a specified column.
 *
 * @param frame Reference to the input data frame
 * @param columnName Column name
 * @param k Number of entries to return (if negative, return the bottom K values, else return top K)
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 */
case class TopK(frame: FrameReference, columnName: String, k: Int,
                weightsColumn: Option[String] = None) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(k != 0, "k should not be equal to zero")
}

