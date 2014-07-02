package com.intel.intelanalytics.service.v1.viewmodels

import spray.json.JsObject

import scala.util.parsing.json.JSONObject

case class QueryResult(part: Long, totalParts: Long, data: JsObject) {
  require(part > 0)
  require(totalParts >= part)
  require(data != null)

}
