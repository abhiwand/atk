package com.intel.intelanalytics.domain.frame

import spray.json.JsValue

/**
 * Command for retrieving the top (or bottom) K distinct values by count for a specified column.
 *
 * @param frame Reference to the input data frame
 * @param columnName Column name
 * @param k Number of entries to return
 * @param reverse Return bottom K entries if true, else return top K
 */
case class TopK(frame: FrameReference, columnName: String, k: Int, reverse: Option[Boolean] = Some(false)) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(k > 0, "k should be greater than zero")
}

