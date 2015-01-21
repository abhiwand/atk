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
import java.util

import com.intel.event.EventLogging
import com.intel.intelanalytics.domain.command.{ CommandDoc, CommandDefinition }
import com.intel.intelanalytics.domain.command.{ CommandPost, CommandDefinition }
import com.intel.intelanalytics.domain.frame.UdfArgs.{ UdfDependency, Udf }
import com.intel.intelanalytics.domain.frame.load.{ LoadFrameArgs, LineParser, LoadSource, LineParserArguments }
import com.intel.intelanalytics.domain.frame.partitioning.{ RepartitionArgs, CoalesceArgs }
import com.intel.intelanalytics.domain.gc.{ GarbageCollectionEntry, GarbageCollection }
import com.intel.intelanalytics.domain.model._
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.frame.load._
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.domain.query.{ RowQuery }
import DataTypes.DataType
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation, QueryPluginResults }
import com.intel.intelanalytics.schema._
import spray.json._
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.graph.construction._
import com.intel.intelanalytics.domain.graph.{ Graph, LoadGraphArgs, GraphReference, GraphTemplate }
import com.intel.intelanalytics.domain.query.RowQuery
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import org.joda.time.{ Duration, DateTime }
import com.intel.intelanalytics.engine.{ ReferenceResolver, ProgressInfo, TaskProgressInfo }
import org.joda.time.DateTime
import com.intel.intelanalytics.engine.{ ProgressInfo, TaskProgressInfo }

import scala.reflect.ClassTag
import scala.util.matching.Regex
import com.intel.intelanalytics.algorithm.Quantile
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import scala.util.Success
import com.intel.intelanalytics.UnitReturn

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.runtime.{ universe => ru }
import ru._
/**
 * Implicit conversions for domain objects to/from JSON
 */

object DomainJsonProtocol extends IADefaultJsonProtocol with EventLogging {

  /**
   * ***********************************************************************
   * NOTE:: Order of implicits matters
   * *************************************************************************
   */

  implicit object DataTypeFormat extends JsonFormat[DataTypes.DataType] {
    override def read(json: JsValue): DataType = {
      val raw = json.asInstanceOf[JsString].value
      //val corrected = raw.substring(1, raw.length - 2)
      DataTypes.toDataType(raw)
    }

    override def write(obj: DataType): JsValue = new JsString(obj.toString)
  }

  implicit val dateTimeFormat = new JsonFormat[DateTime] {
    private val dateTimeFmt = org.joda.time.format.ISODateTimeFormat.dateTime
    def write(x: DateTime) = JsString(dateTimeFmt.print(x))
    def read(value: JsValue) = value match {
      case JsString(x) => dateTimeFmt.parseDateTime(x)
      case x => deserializationError("Expected DateTime as JsString, but got " + x)
    }
  }

  implicit val durationFormat = new JsonFormat[Duration] {
    def write(x: Duration) = JsString(x.toString)
    def read(value: JsValue) = value match {
      case JsString(x) => Duration.parse(x)
      case x => deserializationError("Expected Duration as JsString, but got " + x)
    }
  }

  implicit val columnFormat = jsonFormat3(Column)
  implicit val frameSchemaFormat = jsonFormat(FrameSchema, "columns")
  implicit val vertexSchemaFormat = jsonFormat(VertexSchema, "columns", "label", "id_column_name")
  implicit val edgeSchemaFormat = jsonFormat(EdgeSchema, "columns", "label", "src_vertex_label", "dest_vertex_label", "directed")
  implicit val schemaArgsForamt = jsonFormat1(SchemaArgs)

  /**
   * Format that can handle reading both the current schema class and the old one.
   */
  implicit object SchemaConversionFormat extends JsonFormat[Schema] {

    /** same format as the old one */
    case class LegacySchema(columns: List[(String, DataType)])
    implicit val legacyFormat = jsonFormat1(LegacySchema)

    override def write(obj: Schema): JsValue = obj match {
      case f: FrameSchema => frameSchemaFormat.write(f)
      case v: VertexSchema => vertexSchemaFormat.write(v)
      case e: EdgeSchema => edgeSchemaFormat.write(e)
      case _ => throw new IllegalArgumentException("New type not yet implemented: " + obj.getClass.getName)
    }

    /**
     * Read json
     */
    override def read(json: JsValue): Schema = {
      try {
        if (json.asJsObject.fields.contains("src_vertex_label")) {
          edgeSchemaFormat.read(json)
        }
        else if (json.asJsObject.fields.contains("label")) {
          vertexSchemaFormat.read(json)
        }
        else {
          frameSchemaFormat.read(json)
        }
      }
      catch {
        //  If the new format can't be deserialized, then try the old format that
        // might still be used in the database
        case e: Exception => {
          info("couldn't deserialize schema using any of the current formats, trying old format for json: " + json.compactPrint)
          Schema.fromTuples(legacyFormat.read(json).columns)
        }
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

  class ReferenceFormat[T <: UriReference: TypeTag](entity: EntityType)
      extends JsonFormat[T] {
    override def write(obj: T): JsValue = JsString(obj.uri)

    override def read(json: JsValue): T = {
      implicit val invocation: Invocation = Call(null)
      json match {
        case JsString(name) => ReferenceResolver.resolve[T](name).get
        case JsNumber(n) => ReferenceResolver.resolve[T](s"ia://${entity.name.plural}/$n").get
        case _ => deserializationError(s"Expected valid ${entity.name.plural} URI, but received " + json)
      }
    }
  }

  implicit val frameReferenceFormat = new ReferenceFormat[FrameReference](FrameEntityType)
  implicit def singletonOrListFormat[T: JsonFormat] = new JsonFormat[SingletonOrListValue[T]] {
    def write(list: SingletonOrListValue[T]) = JsArray(list.value.map(_.toJson))
    def read(value: JsValue): SingletonOrListValue[T] = value match {
      case JsArray(list) => SingletonOrListValue[T](list.map(_.convertTo[T]))
      case singleton => SingletonOrListValue[T](List(singleton.convertTo[T]))
    }
  }

  implicit val udfDependenciesFormat = jsonFormat2(UdfDependency)
  implicit val udfFormat = jsonFormat2(Udf)

  /**
   * Convert Java collections to Json.
   */

  //These JSONFormats needs to be before the DataTypeJsonFormat which extends JsonFormat[Any]
  implicit def javaSetFormat[T: JsonFormat: TypeTag] = javaCollectionFormat[java.util.Set[T], T]
  implicit def javaListFormat[T: JsonFormat: TypeTag] = javaCollectionFormat[java.util.List[T], T]

  def javaCollectionFormat[E <: java.util.Collection[T]: TypeTag, T: JsonFormat: TypeTag]: JsonFormat[E] = new JsonFormat[E] {
    override def read(json: JsValue): E = json match {
      case JsArray(elements) =>
        val collection = typeOf[E] match {
          case t if t =:= typeOf[java.util.Set[T]] => new java.util.HashSet[T]()
          case t if t =:= typeOf[java.util.List[T]] => new util.ArrayList[T]()
          case x => deserializationError(s"Unable to deserialize Java collections of type $x")
        }
        val javaCollection = elements.map(_.convertTo[T]).asJavaCollection
        collection.addAll(javaCollection)
        collection.asInstanceOf[E]
      case x => deserializationError(s"Expected a Java collection, but received $x")
    }

    override def write(obj: E): JsValue = obj match {
      case javaCollection: E => javaCollection.asScala.toJson
      case x => serializationError(s"Expected a Java collection, but received: $x")
    }
  }

  /**
   * Convert Java maps to Json.
   */
  //These JSONFormats needs to be before the DataTypeJsonFormat which extends JsonFormat[Any]
  implicit def javaHashMapFormat[K: JsonFormat, V: JsonFormat] = javaMapFormat[java.util.HashMap[K, V], K, V]
  implicit def javaUtilMapFormat[K: JsonFormat, V: JsonFormat] = javaMapFormat[java.util.Map[K, V], K, V]

  def javaMapFormat[M <: java.util.Map[K, V]: ClassTag, K: JsonFormat, V: JsonFormat]: JsonFormat[M] = new JsonFormat[M] {
    override def read(json: JsValue): M = json match {
      case x: JsObject =>
        val javaMap = new java.util.HashMap[K, V]()
        x.fields.map {
          case (key, value) =>
            javaMap.put(JsString(key).convertTo[K], value.convertTo[V])
        }
        javaMap.asInstanceOf[M]

      case x => deserializationError(s"Expected a Java map, but received $x")
    }

    override def write(obj: M): JsValue = obj match {
      case javaMap: java.util.Map[K, V] => {
        val map = javaMap.map {
          case (key, value) =>
            (key.toString, value.toJson)
        }.toMap
        JsObject(map)
      }
      case x => serializationError(s"Expected a Java map, but received: $x")
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
  implicit val longValueFormat = jsonFormat1(LongValue)
  implicit val intValueFormat = jsonFormat1(IntValue)
  implicit val stringValueFormat = jsonFormat1(StringValue)
  implicit val boolValueFormat = jsonFormat1(BoolValue)

  implicit val createEntityArgsFormat = jsonFormat3(CreateEntityArgs.apply)

  implicit val userFormat = jsonFormat5(User)
  implicit val statusFormat = jsonFormat5(Status.apply)
  implicit val dataFrameTemplateFormat = jsonFormat2(DataFrameTemplate)
  implicit val separatorArgsJsonFormat = jsonFormat1(SeparatorArgs)
  implicit val definitionFormat = jsonFormat3(Definition)
  implicit val operationFormat = jsonFormat2(Operation)
  implicit val partialJsFormat = jsonFormat2(Partial[JsObject])
  implicit val loadLinesFormat = jsonFormat6(LoadLines[JsObject])
  implicit val loadLinesLongFormat = jsonFormat6(LoadLines[JsObject])
  implicit val loadSourceParserArgumentsFormat = jsonFormat3(LineParserArguments)
  implicit val loadSourceParserFormat = jsonFormat2(LineParser)
  implicit val loadSourceFormat = jsonFormat6(LoadSource)
  implicit val loadFormat = jsonFormat2(LoadFrameArgs)
  implicit val filterPredicateFormat = jsonFormat2(FilterArgs)
  implicit val removeColumnFormat = jsonFormat2(DropColumnsArgs)
  implicit val addColumnFormat = jsonFormat4(AddColumnsArgs)
  implicit val renameFrameFormat = jsonFormat2(RenameFrameArgs)
  implicit val renameColumnsFormat = jsonFormat2(RenameColumnsArgs)
  implicit val joinFrameLongFormat = jsonFormat3(JoinArgs)
  implicit val groupByColumnFormat = jsonFormat3(GroupByArgs)
  implicit val copyWhereFormat = jsonFormat2(CountWhereArgs)

  implicit val errorFormat = jsonFormat5(Error)
  implicit val flattenColumnLongFormat = jsonFormat3(FlattenColumnArgs)
  implicit val dropDuplicatesFormat = jsonFormat2(DropDuplicatesArgs)
  implicit val taskInfoFormat = jsonFormat1(TaskProgressInfo)
  implicit val progressInfoFormat = jsonFormat2(ProgressInfo)
  implicit val binColumnFormat = jsonFormat5(BinColumnArgs)
  implicit val sortByColumnsFormat = jsonFormat2(SortByColumnsArgs)

  implicit val columnSummaryStatisticsFormat = jsonFormat4(ColumnSummaryStatisticsArgs)
  implicit val columnSummaryStatisticsReturnFormat = jsonFormat13(ColumnSummaryStatisticsReturn)
  implicit val columnFullStatisticsFormat = jsonFormat3(ColumnFullStatisticsArgs)
  implicit val columnFullStatisticsReturnFormat = jsonFormat15(ColumnFullStatisticsReturn)

  implicit val columnModeFormat = jsonFormat4(ColumnModeArgs)
  implicit val columnModeReturnFormat = jsonFormat4(ColumnModeReturn)

  implicit val columnMedianFormat = jsonFormat3(ColumnMedianArgs)
  implicit val columnMedianReturnFormat = jsonFormat1(ColumnMedianReturn)

  implicit val rowQueryFormat = jsonFormat3(RowQuery[Long])
  implicit val queryResultsFormat = jsonFormat2(QueryPluginResults)

  implicit val cumulativeSumFormat = jsonFormat2(CumulativeSumArgs)
  implicit val cumulativePercentSumFormat = jsonFormat2(CumulativePercentArgs)
  implicit val cumulativeCountFormat = jsonFormat3(TallyArgs)
  implicit val cumulativePercentCountFormat = jsonFormat3(TallyPercentArgs)

  implicit val assignSampleFormat = jsonFormat5(AssignSampleArgs)
  implicit val calculatePercentilesFormat = jsonFormat3(QuantilesArgs)
  implicit val calculateCovarianceMatrix = jsonFormat3(CovarianceMatrixArgs)
  implicit val calculateCovariance = jsonFormat2(CovarianceArgs)
  implicit val covarianceReturnFormat = jsonFormat1(DoubleValue)

  implicit val calculateCorrelationMatrix = jsonFormat3(CorrelationMatrixArgs)
  implicit val calculateCorrelation = jsonFormat2(CorrelationArgs)

  implicit val entropyFormat = jsonFormat3(EntropyArgs)
  implicit val entropyReturnFormat = jsonFormat1(EntropyReturn)

  implicit val topKFormat = jsonFormat4(TopKArgs)
  implicit val exportHdfsCsvPlugin = jsonFormat5(ExportHdfsCsvArgs)
  implicit val exportHdfsJsonPlugin = jsonFormat4(ExportHdfsJsonArgs)
  // model performance formats

  implicit val classificationMetricLongFormat = jsonFormat5(ClassificationMetricArgs)
  implicit val classificationMetricValueLongFormat = jsonFormat5(ClassificationMetricValue)
  implicit val ecdfLongFormat = jsonFormat3(EcdfArgs)
  implicit val commandActionFormat = jsonFormat1(CommandPost)

  // model service formats
  implicit val ModelReferenceFormat = new ReferenceFormat[ModelReference](ModelEntityType)
  implicit val modelTemplateFormat = jsonFormat2(ModelTemplate)
  implicit val modelRenameFormat = jsonFormat2(RenameModelArgs)
  implicit val modelFormat = jsonFormat11(ModelEntity)
  implicit val modelLoadFormat = jsonFormat4(ModelLoad)
  implicit val modelPredictFormat = jsonFormat3(ModelPredict)
  implicit val kmeansModelLoadFormat = jsonFormat7(KMeansTrainArgs)
  implicit val kmeansModelPredictFormat = jsonFormat3(KMeansPredictArgs)
  implicit val kmeansModelNewFormat = jsonFormat2(KMeansNewArgs)
  implicit val logisticRegressionModelNewArgsFormat = jsonFormat2(LogisticRegressionWithSGDNewArgs)

  implicit val coalesceArgsFormat = jsonFormat3(CoalesceArgs)
  implicit val repartitionArgsFormat = jsonFormat2(RepartitionArgs)
  implicit val frameNoArgsFormat = jsonFormat1(FrameNoArgs)

  // graph service formats
  implicit val graphReferenceFormat = new ReferenceFormat[GraphReference](GraphEntityType)
  implicit val graphTemplateFormat = jsonFormat2(GraphTemplate)
  implicit val graphRenameFormat = jsonFormat2(RenameGraphArgs)

  implicit val graphNoArgsFormat = jsonFormat1(GraphNoArgs)

  implicit val schemaListFormat = jsonFormat1(SchemaList)

  // graph loading formats for specifying graphbuilder and graphload rules

  implicit val valueFormat = jsonFormat2(ValueRule)
  implicit val propertyFormat = jsonFormat2(PropertyRule)
  implicit val edgeRuleFormat = jsonFormat5(EdgeRule)
  implicit val vertexRuleFormat = jsonFormat2(VertexRule)
  implicit val frameRuleFormat = jsonFormat3(FrameRule)
  implicit val graphLoadFormat = jsonFormat3(LoadGraphArgs)
  implicit val quantileFormat = jsonFormat2(Quantile)
  implicit val QuantileCalculationResultFormat = jsonFormat1(QuantileValues)
  implicit val defineVertexFormat = jsonFormat2(DefineVertexArgs)
  implicit val defineEdgeFormat = jsonFormat5(DefineEdgeArgs)
  implicit val addVerticesFormat = jsonFormat4(AddVerticesArgs)
  implicit val addEdgesFormat = jsonFormat6(AddEdgesArgs)
  implicit val getAllGraphFramesFormat = jsonFormat1(GetAllGraphFrames)
  implicit val filterVertexRowsFormat = jsonFormat2(FilterVerticesArgs)

  implicit val exportGraphFormat = jsonFormat2(ExportGraph)

  // garbage collection formats

  implicit val gcFormat = jsonFormat7(GarbageCollection)
  implicit val gcEntryFormat = jsonFormat7(GarbageCollectionEntry)

  implicit object UnitReturnJsonFormat extends RootJsonFormat[UnitReturn] {
    override def write(obj: UnitReturn): JsValue = {
      JsObject()
    }

    override def read(json: JsValue): UnitReturn = {
      throw new RuntimeException("UnitReturn type should never be provided as an argument")
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
  implicit object FrameCopyFormat extends JsonFormat[CopyFrameArgs] {
    override def read(value: JsValue): CopyFrameArgs = {
      val jo = value.asJsObject
      val frame = frameReferenceFormat.read(jo.getFields("frame")(0))
      val columns: Option[Map[String, String]] = jo.getFields("columns") match {
        case Seq(JsString(n)) => Some(Map[String, String](n -> n))
        case Seq(JsArray(names)) => Some((for (n <- names) yield (n.convertTo[String], n.convertTo[String])).toMap)
        case Seq(JsObject(fields)) => Some((for ((name, new_name) <- fields) yield (name, new_name.convertTo[String])).toMap)
        case Seq(JsNull) => None
        case Seq() => None
        case x => deserializationError(s"Expected FrameCopy JSON string, array, or object for argument 'columns' but got $x")
      }
      val where: Option[Udf] = jo.getFields("where") match {
        case Seq(JsObject(fields)) => Some(Udf(fields("function").convertTo[String], fields("dependencies").convertTo[List[UdfDependency]]))
        case Seq(JsNull) => None
        case Seq() => None
        case x => deserializationError(s"Expected FrameCopy JSON expression for argument 'where' but got $x")
      }
      val name: Option[String] = jo.getFields("name") match {
        case Seq(JsString(n)) => Some(n)
        case Seq(JsNull) => None
      }
      CopyFrameArgs(frame, columns, where, name)
    }

    override def write(frameCopy: CopyFrameArgs): JsValue = frameCopy match {
      case CopyFrameArgs(frame, columns, where, name) => JsObject("frame" -> frame.toJson,
        "columns" -> columns.toJson,
        "where" -> where.toJson,
        "name" -> name.toJson)
    }
  }

  lazy implicit val commandDefinitionFormat = jsonFormat4(CommandDefinition)

  implicit object dataFrameFormat extends JsonFormat[FrameEntity] {
    implicit val dataFrameFormatOriginal = jsonFormat19(FrameEntity)

    override def read(value: JsValue): FrameEntity = {
      dataFrameFormatOriginal.read(value)
    }

    override def write(frame: FrameEntity): JsValue = {
      JsObject(dataFrameFormatOriginal.write(frame).asJsObject.fields +
        ("ia_uri" -> JsString(frame.uri)) +
        ("entity_type" -> JsString(frame.entityType)))
    }
  }

  implicit object graphFormat extends JsonFormat[Graph] {
    implicit val graphFormatOriginal = jsonFormat13(Graph)

    override def read(value: JsValue): Graph = {
      graphFormatOriginal.read(value)
    }

    override def write(graph: Graph): JsValue = {
      JsObject(graphFormatOriginal.write(graph).asJsObject.fields +
        ("ia_uri" -> JsString(graph.uri)) +
        ("entity_type" -> JsString(graph.entityType)))
    }
  }

  implicit val seamlessGraphMetaFormat = jsonFormat2(SeamlessGraphMeta)

}
