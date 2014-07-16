package com.intel.intelanalytics.engine.spark.graph.query

import com.tinkerpop.blueprints.util.io.graphson._
import com.tinkerpop.blueprints.{Element, Graph}
import com.tinkerpop.pipes.util.structures.Row
import spray.json._

import scala.collection.JavaConversions._

/**
 * Implicit conversions for Gremlin query objects to JSON
 */
object GremlinJsonProtocol extends DefaultJsonProtocol {

  /**
   * Convert Blueprints graph elements to GraphSON.
   *
   * GraphSON is a JSON-based format for individual graph elements (i.e. vertices and edges).
   *
   * @param graph Graph used for de-serializing JSON (not needed when serializing elements to JSON)
   * @param mode GraphSON mode
   */
  class GraphSONFormat(graph: Graph = null, mode: GraphSONMode = GraphSONMode.NORMAL) extends JsonFormat[Element] {

    override def read(json: JsValue): Element = json match {
      case x if graph == null => deserializationError(s"No valid graph specified for de-serializing graph elements")
      case x if isGraphElement(x) => elementFromJson(graph, x, mode)
      case x => deserializationError(s"Expected a Blueprints graph element, but received: $x")
    }

    override def write(obj: Element): JsValue = obj match {
      case element: Element => {
        val jsonStr = GraphSONUtility.jsonFromElement(element, null, mode).toString()
        JsonParser(jsonStr)
      }
      case x => serializationError(s"Expected a Blueprints graph element, but received: $x")
    }
  }

  /**
   * Convert Blueprints rows to a Json.
   *
   * A Blueprints row is a list of column names and values. The row is serialized to
   * a Json Map where the column names are keys, and the column values are values.
   */
  implicit def blueprintsRowFormat[T: JsonFormat] = new JsonFormat[Row[T]] {
    override def read(json: JsValue): Row[T] = json match {
      case obj: JsObject => {
        val rowMap = obj.fields.map { field =>
          (field._1.toString, field._2.convertTo[T])
        }
        val columnNames = rowMap.keys.toList
        val columnValues = rowMap.values.toList
        new Row(columnValues, columnNames)
      }
      case x => deserializationError(s"Expected a Blueprints row, but received $x")
    }

    override def write(obj: Row[T]): JsValue = obj match {
      case row: Row[T] => {
        val obj = row.getColumnNames().map(column => {
          new JsField(column, row.getColumn(column).toJson)
        }).toMap
        obj.toJson
      }
      case x => serializationError(s"Expected a blueprints graph element, but received: $x")
    }
  }

  /**
   * Check if JSON contains a Blueprints graph element encoded in GraphSON format.
   */
  def isGraphElement(json: JsValue): Boolean = isEdge(json) | isVertex(json)

  /**
   * Check if JSON contains a Blueprints edge encoded in GraphSON format.
   */
  private def isEdge(json: JsValue): Boolean = json match {
    case obj: JsObject => {
      val elementType = obj.fields.get(GraphSONTokens._TYPE).get.convertTo[String]
      elementType.equalsIgnoreCase(GraphSONTokens.EDGE)
    }
    case _ => false
  }

  /**
   * Check if JSON contains a Blueprints vertex encoded in GraphSON format.
   */
  private def isVertex(json: JsValue): Boolean = json match {
    case obj: JsObject => {
      val elementType = obj.fields.get(GraphSONTokens._TYPE).getOrElse(
        throw new RuntimeException(s"Expected valid GraphSON, but received: ${obj}")
      ).toString
      elementType.equalsIgnoreCase(GraphSONTokens.VERTEX)
    }
    case _ => false
  }

  /**
   * Create Blueprints graph element from JSON. Returns null if not a valid graph element
   */
  private def elementFromJson(graph: Graph, json: JsValue, mode: GraphSONMode = GraphSONMode.NORMAL): Element = {
    val factory = new GraphElementFactory(graph)

    json match {
      case v if isVertex(v) => GraphSONUtility.vertexFromJson(v.toString, factory, mode, null)
      case e if isEdge(e) => {
        val inId = getElementIdFromGraphSON(e, GraphSONTokens._IN_V)
        val outId = getElementIdFromGraphSON(e, GraphSONTokens._OUT_V)
        val outVertex = graph.getVertex(inId)
        val inVertex = graph.getVertex(outId)

        if (inVertex != null && outVertex != null) {
          GraphSONUtility.edgeFromJson(e.toString, outVertex, inVertex, factory, mode, null)
        }
        else throw new RuntimeException(s"Unable to convert JSON to Blueprint's edge: ${e}")
      }
      case x => throw new RuntimeException(s"Unable to convert JSON to Blueprint's graph element: ${x}")
    }
  }

  /**
   * Get element ID from GraphSON
   */
  private def getElementIdFromGraphSON(json: JsValue, idName: String): Long = {
    try {
      getJsonFieldValue(json, idName).toString.toLong
    }
    catch {
      case e: Exception => throw new RuntimeException(s"Unable to get element Id from GraphSON: ${json}", e)
    }
  }

  /**
   * Get field value from JSON object using key.
   */
  private def getJsonFieldValue(json: JsValue, key: String): Any = json match {
    case obj: JsObject => {
      obj.fields.get(key).getOrElse(
        throw new RuntimeException(s"${key} does not exist in JSON object: ${json}")
      )
    }
    case _ => throw new RuntimeException("Invalid JSON object: ${json}")
  }
}
