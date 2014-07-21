package com.intel.intelanalytics.engine

/**
 * represent extra progress information
 * @param retries current number of failed tasks. This is same as error in Spark.
 *                Call it retries because it is a better user experience.
 */
case class TaskProgressInfo(retries: Int)