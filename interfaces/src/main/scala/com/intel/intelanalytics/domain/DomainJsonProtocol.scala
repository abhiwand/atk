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

package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.DataTypes.DataType
import spray.json._
import com.intel.intelanalytics.domain.graphconstruction._
import com.intel.intelanalytics.domain.graphconstruction.Value
import com.intel.intelanalytics.domain.graphconstruction.OutputConfiguration
import com.intel.intelanalytics.domain.graphconstruction.EdgeRule
import com.intel.intelanalytics.domain.graphconstruction.Property

object DomainJsonProtocol extends DefaultJsonProtocol {

  implicit object DataTypeFormat extends JsonFormat[DataTypes.DataType] {
    override def read(json: JsValue): DataType = {
      val raw = json.asInstanceOf[JsString].value
      //val corrected = raw.substring(1, raw.length - 2)
      DataTypes.toDataType(raw)
    }

    override def write(obj: DataType): JsValue = new JsString(obj.toString)
  }

  implicit val schemaFormat = jsonFormat1(Schema)

  implicit val dataFrameFormat = jsonFormat3(DataFrame)
  implicit val dataFrameTemplateFormat = jsonFormat2(DataFrameTemplate)
  implicit val separatorArgsJsonFormat = jsonFormat1(SeparatorArgs)
  implicit val definitionFormat = jsonFormat3(Definition)
  implicit val operationFormat = jsonFormat2(Operation)
  implicit val partialJsFormat = jsonFormat2(Partial[JsObject])
  implicit val loadLinesFormat = jsonFormat4(LoadLines[JsObject, String])
  implicit val loadLinesLongFormat = jsonFormat4(LoadLines[JsObject, Long])
  implicit val filterPredicateFormat = jsonFormat2(FilterPredicate[JsObject, String])
  implicit val filterPredicateLongFormat = jsonFormat2(FilterPredicate[JsObject, Long])
  implicit val errorFormat = jsonFormat5(Error)
  implicit val userFormat = jsonFormat2(User)

  // graph

  implicit val outputConfigurationFormat = jsonFormat2(OutputConfiguration)

  implicit val valueFormat = jsonFormat2(Value)
  implicit val propertyFormat = jsonFormat2(Property)
  implicit val edgeRuleFormat = jsonFormat4(EdgeRule)
  implicit val vertexRuleFormat = jsonFormat2(VertexRule)

  implicit val graphTemplateFormat = jsonFormat7(GraphTemplate)
  implicit val graphFormat = jsonFormat2(Graph)

  implicit object DataTypeJsonFormat extends JsonFormat[Any] {
    override def write(obj: Any): JsValue = {
      obj match {
        case n: Int => new JsNumber(n)
        case n: Long => new JsNumber(n)
        case n: Float => new JsNumber(n)
        case n: Double => new JsNumber(n)
        case s: String => new JsString(s)
        case unk => serializationError("Cannot serialize " + unk.getClass.getName)
      }
    }

    override def read(json: JsValue): Any = {
      json match {
        case JsNumber(n) if n.isValidInt => n.intValue()
        case JsNumber(n) if n.isValidLong => n.longValue()
        case JsNumber(n) if n.isValidFloat => n.floatValue()
        case JsNumber(n) => n.doubleValue()
        case JsString(s) => s
        case unk => serializationError("Cannot deserialize " + unk.getClass.getName)
      }
    }
  }

}
