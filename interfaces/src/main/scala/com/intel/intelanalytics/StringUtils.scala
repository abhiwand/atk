package com.intel.intelanalytics

/**
 *
 */
object StringUtils {

  /**
   * Check if the supplied string is alpha numeric with underscores (used for column names, etc)
   */
  def isAlphanumericUnderscore(str: String): Boolean = {
    for (c <- str.iterator) {
      // Not sure if this is great but it is probably faster than regex
      // http://stackoverflow.com/questions/12831719/fastest-way-to-check-a-string-is-alphanumeric-in-java
      if (c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c < 0x5f) || (c > 0x5f && c <= 0x60) || c > 0x7a) {
        return false
      }
    }
    true
  }
}
