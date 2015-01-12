package com.intel.intelanalytics.domain.frame

/**
 * Arguments for SortByColumns - a list of columns to sort on
 * @param frame the frame to modify
 * @param columnNamesAndAscending column names to sort by, true for ascending, false for descending
 */
case class SortByColumnsArgs(frame: FrameReference, columnNamesAndAscending: List[(String, Boolean)]) {
  require(frame != null, "frame is required")
  require(columnNamesAndAscending != null && columnNamesAndAscending.length > 0, "one or more columnNames is required")
}
