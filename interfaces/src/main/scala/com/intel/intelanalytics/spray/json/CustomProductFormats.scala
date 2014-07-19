package com.intel.intelanalytics.spray.json

import spray.json.{ StandardFormats, ProductFormats }
import org.apache.commons.lang3.StringUtils

/**
 * Override the default behavior in ProductFormats to convert case class property names
 * to lower_case_with_underscores names in JSON.
 */
trait CustomProductFormats extends ProductFormats {
  // StandardFormats is required by ProductFormats
  this: StandardFormats =>

  /**
   * Override the default behavior in ProductFormats to convert case class property names
   * to lower_case_with_underscores names in JSON.
   */
  override protected def extractFieldNames(classManifest: ClassManifest[_]): Array[String] = {
    super.extractFieldNames(classManifest)
      .map(name => JsonPropertyNameConverter.camelCaseToUnderscores(name))
  }

}