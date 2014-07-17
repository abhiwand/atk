package com.intel.intelanalytics.engine

/**
 * represent extra progress information
 * @param succeeded current number of succeeded tasks
 * @param retried current number of failed tasks
 */
case class TaskProgressInfo(succeeded: Int, retried: Int)