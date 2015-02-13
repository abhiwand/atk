package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import com.intel.intelanalytics.domain.frame.FrameReference

/**
 * Arguments for dot-product plugin
 *
 * The dot-product plugin uses the left column values, and the right column values to compute the dot product for each row.
 *
 * @param frame Data frame
 * @param leftColumnNames Left column names
 * @param rightColumnNames Right column names
 * @param dotProductColumnName Name of column for storing results of dot-product
 * @param defaultLeftValues Optional default values used to substitute null values in the left columns
 * @param defaultRightValues Optional default values used to substitute null values in the right columns
 */
case class DotProductArgs(frame: FrameReference,
                          leftColumnNames: List[String],
                          rightColumnNames: List[String],
                          dotProductColumnName: String,
                          defaultLeftValues: Option[List[Double]] = None,
                          defaultRightValues: Option[List[Double]] = None) {
  require(frame != null, "frame is required")
  require(leftColumnNames.size > 0, "number of left columns cannot be zero")
  require(leftColumnNames.size == rightColumnNames.size, "number of left columns should equal number of right columns")
  require(dotProductColumnName != null, "dot product column name cannot be null")
  require(defaultLeftValues.isEmpty || defaultLeftValues.get.size == leftColumnNames.size,
    "size of default left values should equal number of left columns")
  require(defaultRightValues.isEmpty || defaultRightValues.get.size == rightColumnNames.size,
    "size of default right values should equal number of right columns")
}
