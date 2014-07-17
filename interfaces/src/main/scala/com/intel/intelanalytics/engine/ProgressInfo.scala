package com.intel.intelanalytics.engine

/**
 * Progress info for job
 * @param progress current progress
 * @param tasksInfo information regarding task details
 */
case class ProgressInfo(progress: Float, tasksInfo: TaskProgressInfo)
