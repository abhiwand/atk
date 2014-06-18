//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain.frame.load.{Load, LineParser, LoadSource, LineParserArguments}
import com.intel.intelanalytics.domain.schema.{Schema, DataTypes}
import DataTypes.DataType
import spray.json._
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.frame.FrameProject
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.domain.frame.FrameRenameFrame
import com.intel.intelanalytics.domain.graph.construction.ValueRule
import com.intel.intelanalytics.domain.graph.construction.FrameRule
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.domain.frame.FrameAddColumns
import com.intel.intelanalytics.domain.frame.FrameRenameColumn
import com.intel.intelanalytics.domain.frame.FlattenColumn
import com.intel.intelanalytics.domain.frame.FrameRemoveColumn
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.frame.FrameJoin
import com.intel.intelanalytics.domain.graph.GraphLoad
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.frame.LoadLines
import com.intel.intelanalytics.domain.command.Als
import com.intel.intelanalytics.domain.graph.construction.EdgeRule
import com.intel.intelanalytics.domain.graph.construction.PropertyRule
import com.intel.intelanalytics.domain.graph.construction.VertexRule
import org.joda.time.DateTime

/**
 * Implicit conversions for domain objects to JSON
 */
object DomainJsonProtocol extends DefaultJsonProtocol {

  implicit object DataTypeFormat extends JsonFormat[DataTypes.DataType] {
    override def read(json: JsValue): DataType = {
      val raw = json.asInstanceOf[JsString].value
      //val corrected = raw.substring(1, raw.length - 2)
      DataTypes.toDataType(raw)
    }

    override def write(obj: DataType): JsValue = new JsString(obj.toString)
  }

  // TODO: this was added for Joda DateTimes - not sure this is right?
  trait DateTimeJsonFormat extends JsonFormat[DateTime] {
    private val dateTimeFmt = org.joda.time.format.ISODateTimeFormat.dateTime
    def write(x: DateTime) = JsString(dateTimeFmt.print(x))
    def read(value: JsValue) = value match {
      case JsString(x) => dateTimeFmt.parseDateTime(x)
      case x => deserializationError("Expected DateTime as JsString, but got " + x)
    }
  }

  implicit val dateTimeFormat = new DateTimeJsonFormat {}

  implicit val schemaFormat = jsonFormat1(Schema)

  implicit val userFormat = jsonFormat5(User)
  implicit val statusFormat = jsonFormat5(Status)
  implicit val dataFrameFormat = jsonFormat10(DataFrame)
  implicit val dataFrameTemplateFormat = jsonFormat2(DataFrameTemplate)
  implicit val separatorArgsJsonFormat = jsonFormat1(SeparatorArgs)
  implicit val definitionFormat = jsonFormat3(Definition)
  implicit val operationFormat = jsonFormat2(Operation)
  implicit val partialJsFormat = jsonFormat2(Partial[JsObject])
  implicit val loadLinesFormat = jsonFormat6(LoadLines[JsObject, String])
  implicit val loadLinesLongFormat = jsonFormat6(LoadLines[JsObject, Long])
  implicit val loadSourceParserArgumentsFormat = jsonFormat3(LineParserArguments)
  implicit val loadSourceParserFormat = jsonFormat2(LineParser)
  implicit val loadSourceFormat = jsonFormat3(LoadSource)
  implicit val loadFormat = jsonFormat2(Load[String])
  implicit val loadLongFormat = jsonFormat2(Load[Long])
  implicit val filterPredicateFormat = jsonFormat2(FilterPredicate[JsObject, String])
  implicit val filterPredicateLongFormat = jsonFormat2(FilterPredicate[JsObject, Long])
  implicit val removeColumnFormat = jsonFormat2(FrameRemoveColumn[JsObject, String])
  implicit val removeColumnLongFormat = jsonFormat2(FrameRemoveColumn[JsObject, Long])
  implicit val addColumnFormat = jsonFormat4(FrameAddColumns[JsObject, String])
  implicit val addColumnLongFormat = jsonFormat4(FrameAddColumns[JsObject, Long])
  implicit val projectColumnFormat = jsonFormat4(FrameProject[JsObject, String])
  implicit val projectColumnLongFormat = jsonFormat4(FrameProject[JsObject, Long])
  implicit val renameFrameFormat = jsonFormat2(FrameRenameFrame[JsObject, String])
  implicit val renameFrameLongFormat = jsonFormat2(FrameRenameFrame[JsObject, Long])
  implicit val renameColumnFormat = jsonFormat3(FrameRenameColumn[JsObject, String])
  implicit val renameColumnLongFormat = jsonFormat3(FrameRenameColumn[JsObject, Long])
  implicit val joinFrameLongFormat = jsonFormat3(FrameJoin)
  implicit val groupByColumnFormat = jsonFormat4(FrameGroupByColumn[JsObject, String])
  implicit val groupByColumnLongFormat = jsonFormat4(FrameGroupByColumn[JsObject, Long])

  implicit val alsFormatString = jsonFormat5(Als[String])
  implicit val alsFormatLong = jsonFormat5(Als[Long])
  implicit val errorFormat = jsonFormat5(Error)
  implicit val flattenColumnLongFormat = jsonFormat4(FlattenColumn[Long])

  // graph service formats

  implicit val graphTemplateFormat = jsonFormat1(GraphTemplate)
  implicit val graphFormat = jsonFormat9(Graph)

  // graph loading formats for specifying graphbuilder and graphload rules

  implicit val valueFormat = jsonFormat2(ValueRule)
  implicit val propertyFormat = jsonFormat2(PropertyRule)
  implicit val edgeRuleFormat = jsonFormat5(EdgeRule)
  implicit val vertexRuleFormat = jsonFormat2(VertexRule)
  implicit val frameRuleLongs = jsonFormat3(FrameRule[Long])
  implicit val frameRuleStrings = jsonFormat3(FrameRule[String])
  implicit val graphLoadLongs = jsonFormat3(GraphLoad[JsObject, Long, Long])
  implicit val graphLoadStrings = jsonFormat3(GraphLoad[JsObject, String, String])

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
