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

import java.net.URI

import com.intel.intelanalytics.domain.command.CommandDefinition
import com.intel.intelanalytics.domain.frame.load.{ Load, LineParser, LoadSource, LineParserArguments }
import com.intel.intelanalytics.domain.query.{ RowQuery }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import DataTypes.DataType
import com.intel.intelanalytics.schema._
import spray.json._
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.graph.{ GraphReference, Graph, GraphLoad, GraphTemplate }
import com.intel.intelanalytics.domain.graph.construction.{ EdgeRule, FrameRule, PropertyRule, ValueRule, VertexRule }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import org.joda.time.DateTime
import spray.json._
import com.intel.intelanalytics.engine.ProgressInfo

import scala.util.matching.Regex

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

  implicit object FileNameFormat extends JsonFormat[FileName] {
    override def write(obj: FileName): JsValue = JsString(obj.name)

    override def read(json: JsValue): FileName = json match {
      case JsString(name) => FileName(name)
      case x => deserializationError("Expected file name, but got " + x)
    }
  }

  /**
   * Holds a regular expression, plus the group number we care about in case
   * the pattern is a match
   */
  case class PatternIndex(pattern: Regex, groupNumber: Int) {
    def findMatch(text: String): Option[String] = {
      val result = pattern.findFirstMatchIn(text)
        .map(m => m.group(groupNumber))
        .flatMap(s => if (s == null) None else Some(s))
      result
    }
  }

  class ReferenceFormat[T <: HasId](patterns: Seq[PatternIndex], collection: String, name: String, factory: Long => T)
      extends JsonFormat[T] {

    override def write(obj: T): JsValue = JsString(s"ia://$collection/${obj.id}")

    override def read(json: JsValue): T = json match {
      case JsString(name) =>
        val id = patterns.flatMap(p => p.findMatch(name))
          .headOption
          .map(s => s.toLong)
          .getOrElse(deserializationError(s"Couldn't find $collection ID in " + name))
        factory(id)
      case JsNumber(n) => factory(n.toLong)
      case _ => deserializationError(s"Expected $name URL, but received " + json)
    }
  }

  implicit val frameReferenceFormat = new ReferenceFormat[FrameReference](
    List(PatternIndex("""ia://dataframes/(\d+)""".r, 1),
      PatternIndex("""https?://.+/dataframes/(\d+)""".r, 1)),
    "dataframes", "data frame",
    n => FrameReference(n))

  implicit val graphReferenceFormat = new ReferenceFormat[GraphReference](
    List(PatternIndex("""ia://graphs/(\d+)""".r, 1),
      PatternIndex("""https?://.+/graphs/(\d+)""".r, 1)),
    "graphs", "graph",
    n => GraphReference(n))

  implicit val userFormat = jsonFormat5(User)
  implicit val statusFormat = jsonFormat5(Status)
  implicit val dataFrameFormat = jsonFormat9(DataFrame)
  implicit val dataFrameTemplateFormat = jsonFormat2(DataFrameTemplate)
  implicit val separatorArgsJsonFormat = jsonFormat1(SeparatorArgs)
  implicit val definitionFormat = jsonFormat3(Definition)
  implicit val operationFormat = jsonFormat2(Operation)
  implicit val partialJsFormat = jsonFormat2(Partial[JsObject])
  implicit val loadLinesFormat = jsonFormat6(LoadLines[JsObject])
  implicit val loadLinesLongFormat = jsonFormat6(LoadLines[JsObject])
  implicit val loadSourceParserArgumentsFormat = jsonFormat3(LineParserArguments)
  implicit val loadSourceParserFormat = jsonFormat2(LineParser)
  implicit val loadSourceFormat = jsonFormat3(LoadSource)
  implicit val loadFormat = jsonFormat2(Load)
  implicit val filterPredicateFormat = jsonFormat2(FilterPredicate[JsObject, String])
  implicit val filterPredicateLongFormat = jsonFormat2(FilterPredicate[JsObject, Long])
  implicit val removeColumnFormat = jsonFormat2(FrameRemoveColumn)
  implicit val addColumnFormat = jsonFormat4(FrameAddColumns[JsObject, String])
  implicit val addColumnLongFormat = jsonFormat4(FrameAddColumns[JsObject, Long])
  implicit val projectColumnFormat = jsonFormat4(FrameProject[JsObject, String])
  implicit val projectColumnLongFormat = jsonFormat4(FrameProject[JsObject, Long])
  implicit val renameFrameFormat = jsonFormat2(FrameRenameFrame)
  implicit val renameColumnFormat = jsonFormat3(FrameRenameColumn[JsObject, String])
  implicit val renameColumnLongFormat = jsonFormat3(FrameRenameColumn[JsObject, Long])
  implicit val joinFrameLongFormat = jsonFormat3(FrameJoin)
  implicit val groupByColumnFormat = jsonFormat4(FrameGroupByColumn[JsObject, String])
  implicit val groupByColumnLongFormat = jsonFormat4(FrameGroupByColumn[JsObject, Long])

  implicit val errorFormat = jsonFormat5(Error)
  implicit val flattenColumnLongFormat = jsonFormat4(FlattenColumn)
  implicit val dropDuplicatesFormat = jsonFormat2(DropDuplicates)
  implicit val progressInfoFormat = jsonFormat2(ProgressInfo)
  implicit val binColumnLongFormat = jsonFormat6(BinColumn[Long])

  implicit val rowQueryFormat = jsonFormat3(RowQuery[Long])

  // model performance formats

  implicit val classificationMetricLongFormat = jsonFormat6(ClassificationMetric[Long])
  implicit val classificationMetricValueLongFormat = jsonFormat1(ClassificationMetricValue)

  // graph service formats

  implicit val graphTemplateFormat = jsonFormat1(GraphTemplate)
  implicit val graphFormat = jsonFormat9(Graph)

  // graph loading formats for specifying graphbuilder and graphload rules

  implicit val valueFormat = jsonFormat2(ValueRule)
  implicit val propertyFormat = jsonFormat2(PropertyRule)
  implicit val edgeRuleFormat = jsonFormat5(EdgeRule)
  implicit val vertexRuleFormat = jsonFormat2(VertexRule)
  implicit val frameRuleFormat = jsonFormat3(FrameRule)
  implicit val graphLoadFormat = jsonFormat3(GraphLoad)

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
        case unk => deserializationError("Cannot deserialize " + unk.getClass.getName)
      }
    }

  }
  implicit object UriFormat extends JsonFormat[URI] {
    override def read(json: JsValue): URI = json match {
      case JsString(value) => new URI(value)
      case x => deserializationError(s"Expected string, received $x")
    }

    override def write(obj: URI): JsValue = JsString(obj.toString)
  }

  implicit object JsonSchemaFormat extends JsonFormat[JsonSchema] {
    override def read(json: JsValue): JsonSchema = json match {
      case JsObject(o) =>
        o.getOrElse("type", JsString("object")) match {
          case JsString("string") => stringSchemaFormat.read(json)
          case JsString("array") => arraySchemaFormat.read(json)
          case JsString("number") => numberSchemaFormat.read(json)
          case _ => objectSchemaFormat.read(json)
        }
      case x => deserializationError(s"Expected a Json schema object, but got $x")
    }

    override def write(obj: JsonSchema): JsValue = obj match {
      case o: ObjectSchema => objectSchemaFormat.write(o)
      case s: StringSchema => stringSchemaFormat.write(s)
      case a: ArraySchema => arraySchemaFormat.write(a)
      case n: NumberSchema => numberSchemaFormat.write(n)
      case JsonSchema.empty => JsObject().toJson
      case x => serializationError(s"Expected a valid json schema object, but received: $x")
    }
  }

  lazy implicit val numberSchemaFormat = jsonFormat9(NumberSchema)
  lazy implicit val stringSchemaFormat = jsonFormat9(StringSchema)
  lazy implicit val objectSchemaFormat = jsonFormat12(ObjectSchema)
  lazy implicit val arraySchemaFormat = jsonFormat9(ArraySchema)
  lazy implicit val commandDefinitionFormat = jsonFormat3(CommandDefinition)
}
