package com.intel.graphbuilder.util

/**
 * Prefer org.apache.commons.lang3.StringUtils over writing your own methods below
 */
object StringUtils {

  /**
   * Call toString() on the supplied object, handling null safely
   * @param any object
   * @return null if called on null object, otherwise result of toString()
   */
  def nullSafeToString(any: Any): String = {
    if (any != null) any.toString()
    else null
  }
}