package com.intel.intelanalytics.domain.frame

/**
 * The args for any frame plugin that takes "no args" except the frame
 * @param frame the frame to operate on
 */
case class FrameNoArgs(frame: FrameReference) {
  require(frame != null, "frame is required")
}
