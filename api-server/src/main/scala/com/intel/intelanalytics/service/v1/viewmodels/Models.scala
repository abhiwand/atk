package com.intel.intelanalytics.service.v1.viewmodels

import com.intel.intelanalytics.domain._
import spray.json.{JsObject, JsValue, DefaultJsonProtocol}
import spray.httpx.SprayJsonSupport
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.domain.Operation
import com.intel.intelanalytics.service.v1.viewmodels.DataFrameHeader
import com.intel.intelanalytics.service.v1.viewmodels.JsonTransform
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.intel.intelanalytics.domain.Schema
import com.intel.intelanalytics.service.v1.viewmodels.LoadLines
import com.intel.intelanalytics.service.v1.viewmodels.DecoratedDataFrame

//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

case class RelLink(rel: String, uri: String, method: String) {
  require(rel != null)
  require(uri != null)
  require(method != null)
  require(List("GET", "PUT", "POST", "HEAD", "DELETE", "OPTIONS").contains(method))
}

object Rel {
  def self(uri: String) = RelLink(rel = "self", uri = uri, method = "GET")
}

case class DecoratedDataFrame(id: Long, name: String, schema: Schema, links: List[RelLink]) {
  require(id > 0)
  require(name != null)
  require(schema != null)
  require(links != null)
}

case class DataFrameHeader(id: Long, name: String, url: String) {
  require(id > 0)
  require(name != null)
  require(url != null)
}

case class JsonTransform(name: String, arguments: Option[JsObject]) {
  require(name != null, "Name is required")
}

case class LoadLines(source: String, destination: String, skipRows: Option[Int], lineParser: Partial[JsObject]) {
  require(source != null, "source is required")
  require(destination != null, "destination is required")
  require(skipRows.isEmpty || skipRows.get >= 0, "cannot skip negative number of rows")
  require(lineParser != null, "lineParser is required")
}

object ViewModelJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._ //this is needed for implicits
  implicit val relLinkFormat = jsonFormat3(RelLink)
  implicit val dataFrameHeaderFormat = jsonFormat3(DataFrameHeader)
  implicit val decoratedDataFrameFormat = jsonFormat4(DecoratedDataFrame)
  implicit val jsonTransformFormat = jsonFormat2(JsonTransform)
  implicit val definitionFormat = jsonFormat3(Definition)
  implicit val operationFormat = jsonFormat2(Operation)
  implicit val partialJsFormat = jsonFormat2(Partial[JsObject])
  implicit val loadLinesFormat = jsonFormat4(LoadLines)
}
