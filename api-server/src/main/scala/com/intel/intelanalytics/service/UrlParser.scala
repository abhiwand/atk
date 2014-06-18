package com.intel.intelanalytics.service

/**
 * Utility method for getting ids out of URL's
 */
object UrlParser {

  private val frameIdRegex = "/dataframes/(\\d+)".r

  /**
   * Get the frameId out of a URL in the format "../dataframes/id"
   * @return unique id
   */
  def getFrameId(url: String): Option[Long] = {
    val id = frameIdRegex.findFirstMatchIn(url).map(m ⇒ m.group(1))
    id.map(s ⇒ s.toLong)
  }

  private val graphIdRegex = "/graphs/(\\d+)".r

  /**
   * Get the graphId out of a URL in the format "../graphs/id"
   * @return unique id
   */
  def getGraphId(url: String): Option[Long] = {
    val id = graphIdRegex.findFirstMatchIn(url).map(m ⇒ m.group(1))
    id.map(s ⇒ s.toLong)
  }

}
