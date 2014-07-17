package com.intel.intelanalytics.engine

/**
 * represent extra progress information
 * @param retries current number of failed tasks
 */
case class TaskProgressInfo(retries: Int)