package com.intel.intelanalytics.domain.query

import com.intel.intelanalytics.engine.Rows.Row
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

case class QueryResult(data: List[JsValue]) {
}

object QueryResultFactory {
  def buildFromRows(rows: Iterable[Row]) = {
    QueryResult(rows.map(row => row.map {
      case null => JsNull
      case a => a.toJson
    }.toJson).toList)
  }
}
