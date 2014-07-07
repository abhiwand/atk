package com.intel.intelanalytics.domain.query

import spray.json.JsValue

/**
 * Result object from requesting a specfic number of rows from a data frame
 * The reason why data is a List[JsValue] rather than a native Scala type is that spray-json does not support the
 * implicit parsing of AnyVal We have to do that manually to get spray-json to work with our Row object which is an Array[Any].
 * @param data
 */
case class RowQueryResult(data: List[JsValue])
