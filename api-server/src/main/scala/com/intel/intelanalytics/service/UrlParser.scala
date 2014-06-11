package com.intel.intelanalytics.service

/**
 * Utility method for getting ids out of URL's
 */
object UrlParser {

  private val frameIdRegex = "/dataframes/(\\d+)".r

  def getFrameId(url: String): Option[Long] = {
    val id = frameIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

  private val graphIdRegex = "/graphs/(\\d+)".r

  def getGraphId(url: String): Option[Long] = {
    val id = graphIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

}
