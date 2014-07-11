package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.Schema
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows.Row

/**
 * RDD results of loading a dataframe including both successfully parsed lines and errors
 * @param parsedLines lines that were successfully parsed
 * @param failedLines lines that were NOT successfully parsed including error messages
 */
case class ParseResultRddWrapper(parsedLines: FrameRDD, failedLines: FrameRDD)

