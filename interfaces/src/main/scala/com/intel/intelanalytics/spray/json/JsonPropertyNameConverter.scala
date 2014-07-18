package com.intel.intelanalytics.spray.json

import org.apache.commons.lang3.StringUtils

/**
 * Convert CamelCase String to lower_case_with_underscores.
 */
object JsonPropertyNameConverter {

  /**
   * Convert CamelCase String to lower_case_with_underscores.
   */
  def camelCaseToUnderscores(camelCase: String): String = {
    if (camelCase == null) {
      return null
    }
    // This is only called when setting up JSON formats for case classes (NOT every time
    // you call toJSON) so it doesn't need to be particularly efficient.
    val parts = StringUtils.splitByCharacterTypeCamelCase(camelCase.trim)
    val mixedCaseWithUnderscores = StringUtils.join(parts.asInstanceOf[Array[AnyRef]], "_")
    val lower = mixedCaseWithUnderscores.toLowerCase
    // remove extra underscores (these might be added if the string already had underscores in it)
    StringUtils.replacePattern(lower, "[_]+", "_")
  }
}
