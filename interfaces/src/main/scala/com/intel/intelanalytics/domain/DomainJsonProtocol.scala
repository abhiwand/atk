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

import com.intel.intelanalytics.domain.command.{ CommandDoc, CommandDefinition }
import com.intel.intelanalytics.domain.command.{ CommandPost, CommandDefinition }
import com.intel.intelanalytics.domain.frame.load._
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.domain.query.{ RowQuery }
import DataTypes.DataType
import com.intel.intelanalytics.engine.plugin.QueryPluginResults
import com.intel.intelanalytics.schema._
import spray.json._
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.graph.construction._
import com.intel.intelanalytics.domain.graph.{ Graph, GraphLoad, GraphReference, GraphTemplate }
import com.intel.intelanalytics.domain.query.RowQuery
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import org.joda.time.DateTime
import com.intel.intelanalytics.engine.{ ProgressInfo, TaskProgressInfo }

import scala.util.matching.Regex
import com.intel.intelanalytics.algorithm.Quantile
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import scala.util.Success
import com.intel.intelanalytics.UnitReturn

/**
 * Implicit conversions for domain objects to/from JSON
 */
object DomainJsonProtocol extends IADefaultJsonProtocol {

  implicit object DataTypeFormat extends JsonFormat[DataTypes.DataType] {
    override def read(json: JsValue): DataType = {
      val raw = json.asInstanceOf[JsString].value
      //val corrected = raw.substring(1, raw.length - 2)
      DataTypes.toDataType(raw)
    }

    override def write(obj: DataType): JsValue = new JsString(obj.toString)
  }

  trait DateTimeJsonFormat extends JsonFormat[DateTime] {
    private val dateTimeFmt = org.joda.time.format.ISODateTimeFormat.dateTime
    def write(x: DateTime) = JsString(dateTimeFmt.print(x))
    def read(value: JsValue) = value match {
      case JsString(x) => dateTimeFmt.parseDateTime(x)
      case x => deserializationError("Expected DateTime as JsString, but got " + x)
    }
  }

  implicit val dateTimeFormat = new DateTimeJsonFormat {}

  implicit val vertexSchemaFormat = jsonFormat2(VertexSchema)
  implicit val edgeSchemaFormat = jsonFormat4(EdgeSchema)
  implicit val columnFormat = jsonFormat3(Column)
  implicit val schemaArgsForamt = jsonFormat1(SchemaArgs)

  /**
   * Format that can handle reading both the current schema class and the old one.
   */
  implicit object SchemaConversionFormat extends JsonFormat[Schema] {

    /** same format as the old one */
    case class LegacySchema(columns: List[(String, DataType)])
    implicit val legacyFormat = jsonFormat1(LegacySchema)
    implicit val schemaFormat = jsonFormat(Schema, "columns", "vertex_schema", "edge_schema")

    override def write(obj: Schema): JsValue = schemaFormat.write(obj)

    /**
     * If the new format can't be deserialized, then try the old format that
     * might still be used in the database
     */
    override def read(json: JsValue): Schema = {
      try {
        schemaFormat.read(json)
      }
      catch {
        case e: Exception => new Schema(legacyFormat.read(json).columns)
      }
    }
  }

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

  class ReferenceFormat[T <: HasId](collection: String, name: String, factory: Long => T)
      extends JsonFormat[T] {
    override def write(obj: T): JsValue = JsString(s"ia://$collection/${obj.id}")

    override def read(json: JsValue): T = json match {
      case JsString(name) =>
        factory(IAUriFactory.getReference(name).id)
      case JsNumber(n) => factory(n.toLong)
      case _ => deserializationError(s"Expected $name URL, but received " + json)
    }
  }

  implicit def singletonOrListFormat[T: JsonFormat] = new JsonFormat[SingletonOrListValue[T]] {
    def write(list: SingletonOrListValue[T]) = JsArray(list.value.map(_.toJson))
    def read(value: JsValue): SingletonOrListValue[T] = value match {
      case JsArray(list) => SingletonOrListValue[T](list.map(_.convertTo[T]))
      case singleton => SingletonOrListValue[T](List(singleton.convertTo[T]))
    }
  }

  implicit val longValueFormat = jsonFormat1(LongValue)
  implicit val stringValueFormat = jsonFormat1(StringValue)

  implicit val frameReferenceFormat = new ReferenceFormat[FrameReference]("frames", "frame", n => FrameReference(n))
  implicit val userFormat = jsonFormat5(User)
  implicit val statusFormat = jsonFormat5(Status)
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
  implicit val filterPredicateFormat = jsonFormat2(FilterPredicate)
  implicit val removeColumnFormat = jsonFormat2(FrameDropColumns)
  implicit val addColumnFormat = jsonFormat4(FrameAddColumns)
  implicit val projectColumnFormat = jsonFormat4(FrameProject)
  implicit val renameFrameFormat = jsonFormat2(RenameFrame)
  implicit val renameColumnsFormat = jsonFormat2(FrameRenameColumns)
  implicit val joinFrameLongFormat = jsonFormat3(FrameJoin)
  implicit val groupByColumnFormat = jsonFormat4(FrameGroupByColumn)
  implicit val copyWhereFormat = jsonFormat2(FrameCountWhere)
  implicit val justALongFormat = jsonFormat1(JustALong)

  implicit val errorFormat = jsonFormat5(Error)
  implicit val flattenColumnLongFormat = jsonFormat4(FlattenColumn)
  implicit val dropDuplicatesFormat = jsonFormat2(DropDuplicates)
  implicit val taskInfoFormat = jsonFormat1(TaskProgressInfo)
  implicit val progressInfoFormat = jsonFormat2(ProgressInfo)
  implicit val binColumnFormat = jsonFormat6(BinColumn)
  implicit val sortByColumnsFormat = jsonFormat2(SortByColumns)

  implicit val columnSummaryStatisticsFormat = jsonFormat4(ColumnSummaryStatistics)
  implicit val columnSummaryStatisticsReturnFormat = jsonFormat13(ColumnSummaryStatisticsReturn)
  implicit val columnFullStatisticsFormat = jsonFormat3(ColumnFullStatistics)
  implicit val columnFullStatisticsReturnFormat = jsonFormat15(ColumnFullStatisticsReturn)

  implicit val columnModeFormat = jsonFormat4(ColumnMode)
  implicit val columnModeReturnFormat = jsonFormat4(ColumnModeReturn)

  implicit val columnMedianFormat = jsonFormat3(ColumnMedian)
  implicit val columnMedianReturnFormat = jsonFormat1(ColumnMedianReturn)

  implicit val rowQueryFormat = jsonFormat3(RowQuery[Long])
  implicit val queryResultsFormat = jsonFormat2(QueryPluginResults)

  implicit val cumulativeSumFormat = jsonFormat2(CumulativeSum)
  implicit val cumulativePercentSumFormat = jsonFormat2(CumulativePercentSum)
  implicit val cumulativeCountFormat = jsonFormat3(CumulativeCount)
  implicit val cumulativePercentCountFormat = jsonFormat3(CumulativePercentCount)

  implicit val assignSampleFormat = jsonFormat5(AssignSample)
  implicit val calculatePercentilesFormat = jsonFormat3(Quantiles)

  implicit val entropyFormat = jsonFormat3(Entropy)
  implicit val entropyReturnFormat = jsonFormat1(EntropyReturn)

  implicit val topKFormat = jsonFormat4(TopK)

  // model performance formats

  implicit val classificationMetricLongFormat = jsonFormat5(ClassificationMetric)
  implicit val classificationMetricValueLongFormat = jsonFormat5(ClassificationMetricValue)
  implicit val ecdfLongFormat = jsonFormat3(ECDF)
  implicit val commandActionFormat = jsonFormat1(CommandPost)

  // graph service formats
  implicit val graphReferenceFormat = new ReferenceFormat[GraphReference]("graphs", "graph", n => GraphReference(n))
  implicit val graphTemplateFormat = jsonFormat2(GraphTemplate)
  implicit val graphRenameFormat = jsonFormat2(RenameGraph)

  implicit val graphNoArgsFormat = jsonFormat1(GraphNoArgs)

  // graph loading formats for specifying graphbuilder and graphload rules

  implicit val valueFormat = jsonFormat2(ValueRule)
  implicit val propertyFormat = jsonFormat2(PropertyRule)
  implicit val edgeRuleFormat = jsonFormat5(EdgeRule)
  implicit val vertexRuleFormat = jsonFormat2(VertexRule)
  implicit val frameRuleFormat = jsonFormat3(FrameRule)
  implicit val graphLoadFormat = jsonFormat3(GraphLoad)
  implicit val quantileFormat = jsonFormat2(Quantile)
  implicit val QuantileCalculationResultFormat = jsonFormat1(QuantileValues)
  implicit val defineVertexFormat = jsonFormat2(DefineVertex)
  implicit val defineEdgeFormat = jsonFormat5(DefineEdge)
  implicit val addVerticesFormat = jsonFormat4(AddVertices)
  implicit val addEdgesFormat = jsonFormat6(AddEdges)
  implicit val getAllGraphFramesFormat = jsonFormat1(GetAllGraphFrames)
  implicit val filterVertexRowsFormat = jsonFormat2(FilterVertexRows)

  implicit object UnitReturnJsonFormat extends RootJsonFormat[UnitReturn] {
    override def write(obj: UnitReturn): JsValue = {
      JsObject()
    }

    override def read(json: JsValue): UnitReturn = {
      throw new RuntimeException("UnitReturn type should never be provided as an argument")
    }
  }

  implicit object DataTypeJsonFormat extends JsonFormat[Any] {
    override def write(obj: Any): JsValue = {
      obj match {
        case n: Int => new JsNumber(n)
        case n: Long => new JsNumber(n)
        case n: Float => new JsNumber(n)
        case n: Double => new JsNumber(n)
        case s: String => new JsString(s)
        case n: java.lang.Long => new JsNumber(n.longValue())
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

  implicit object CommandDocFormat extends JsonFormat[CommandDoc] {
    override def read(value: JsValue): CommandDoc = {
      value.asJsObject.getFields("title", "description") match {
        case Seq(JsString(title), JsString(description)) =>
          CommandDoc(title, Some(description))
        case Seq(JsString(title), JsNull) =>
          CommandDoc(title, None)
        case x => deserializationError(s"Expected a CommandDoc Json object, but got $x")
      }
    }

    override def write(doc: CommandDoc): JsValue = doc.extendedSummary match {
      case Some(d) => JsObject("title" -> JsString(doc.oneLineSummary), "description" -> JsString(doc.extendedSummary.get))
      case None => JsObject("title" -> JsString(doc.oneLineSummary))
    }
  }

  /**
   * Explict JSON handling for FrameCopy where 'columns' arg can be a String, a List, or a Map
   */
  implicit object FrameCopyFormat extends JsonFormat[FrameCopy] {
    override def read(value: JsValue): FrameCopy = {
      val jo = value.asJsObject
      val frame = frameReferenceFormat.read(jo.getFields("frame")(0))
      val columns: Option[Map[String, String]] = jo.getFields("columns") match {
        case Seq(JsString(name)) => Some(Map[String, String](name -> name))
        case Seq(JsArray(names)) => Some((for (n <- names) yield (n.convertTo[String], n.convertTo[String])).toMap)
        case Seq(JsObject(fields)) => Some((for ((name, new_name) <- fields) yield (name, new_name.convertTo[String])).toMap)
        case Seq(JsNull) => None
        case Seq() => None
        case x => deserializationError(s"Expected FrameCopy JSON string, array, or object for argument 'columns' but got $x")
      }
      val where: Option[String] = jo.getFields("where") match {
        case Seq(JsString(expression)) => Some(expression)
        case Seq(JsNull) => None
        case Seq() => None
        case x => deserializationError(s"Expected FrameCopy JSON expression for argument 'where' but got $x")
      }
      FrameCopy(frame, columns, where)
    }

    override def write(frameCopy: FrameCopy): JsValue = frameCopy match {
      case FrameCopy(frame, columns, where) => JsObject("frame" -> frame.toJson,
        "columns" -> columns.toJson,
        "where" -> where.toJson)
    }
  }

  lazy implicit val commandDefinitionFormat = jsonFormat4(CommandDefinition)

  implicit object dataFrameFormat extends JsonFormat[DataFrame] {
    implicit val dataFrameFormatOriginal = jsonFormat13(DataFrame)

    override def read(value: JsValue): DataFrame = {
      dataFrameFormatOriginal.read(value)
    }

    override def write(frame: DataFrame): JsValue = {
      JsObject(dataFrameFormatOriginal.write(frame).asJsObject.fields + ("ia_uri" -> JsString(frame.uri)) + ("command_prefix" -> JsString(frame.commandPrefix)))
    }
  }

  implicit object graphFormat extends JsonFormat[Graph] {
    implicit val graphFormatOriginal = jsonFormat11(Graph)

    override def read(value: JsValue): Graph = {
      graphFormatOriginal.read(value)
    }

    override def write(graph: Graph): JsValue = {
      JsObject(graphFormatOriginal.write(graph).asJsObject.fields + ("ia_uri" -> JsString(graph.uri)) + ("command_prefix" -> JsString(graph.commandPrefix)))
    }
  }

  implicit val seamlessGraphMetaFormat = jsonFormat2(SeamlessGraphMeta)
}
