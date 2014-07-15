package com.intel.intelanalytics.engine.spark.graph.query

import com.tinkerpop.blueprints.util.io.graphson._
import com.tinkerpop.blueprints.{ Element, Graph }
import com.tinkerpop.pipes.util.structures.Row
import spray.json._
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Implicit conversions for Gremlin query objects to JSON
 */
object GremlinJsonProtocol extends DefaultJsonProtocol {

  /**
   * Convert Blueprints graph elements to GraphSON
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
   * Convert Blueprints rows to Json.
   *
   * A Blueprints row is a list of column names and values.
   */
  implicit def blueprintsRowFormat[T: JsonFormat: ClassTag] = new JsonFormat[Row[T]] {
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
        }).toList
        obj.toJson
      }
      case x => serializationError(s"Expected a blueprints graph element, but received: $x")
    }
  }

  /**
   * Convert objects returned from Gremlin queries to JSON
   *
   * @param graph Graph for de-serializing graph elements
   * @param mode GraphSON mode
   */
  class GremlinJsonFormat[T: JsonFormat: ClassTag](graph: Graph = null, mode: GraphSONMode = GraphSONMode.NORMAL) extends JsonFormat[T] {
    implicit val graphSONFormat = new GraphSONFormat(graph, mode)

    override def read(json: JsValue): T = json match {
      case x if isGraphElement(x) => elementFromJson(graph, x).asInstanceOf[T]
      case x => x.convertTo[T]
    }

    override def write(obj: T): JsValue = {
      obj match {
        case e: Element => e.toJson
        case r: Row[T] => r.toJson
        case x => x.toJson
      }
    }
  }

  /**
   * Create Blueprints graph element from JSON. Returns null if not a valid graph element
   */
  private def elementFromJson(graph: Graph, json: JsValue, mode: GraphSONMode = GraphSONMode.NORMAL): Element = {
    val factory = new GraphElementFactory(graph)

    json match {
      case v if isVertex(v) => GraphSONUtility.vertexFromJson(v.toString, factory, mode, null)
      case e if isEdge(e) => {
        val outVertex = graph.getVertex(getElementId(e, GraphSONTokens._OUT_V))
        val inVertex = graph.getVertex(getElementId(e, GraphSONTokens._IN_V))
        if (inVertex != null && outVertex != null) {
          GraphSONUtility.edgeFromJson(e.toString, outVertex, inVertex, factory, mode, null)
        }
        else null
      }
      case _ => null
    }
  }

  /**
   * Check if GraphSON contains a Blueprints edge
   */
  private def isEdge(json: JsValue): Boolean = json match {
    case obj: JsObject => {
      val elementType = obj.fields.get(GraphSONTokens._TYPE).get.convertTo[String]
      elementType.toLowerCase == GraphSONTokens.EDGE
    }
    case _ => false
  }

  /**
   * Check if GraphSON contains a Blueprints vertex
   */
  private def isVertex(json: JsValue): Boolean = json match {
    case obj: JsObject => {
      val elementType = obj.fields.get(GraphSONTokens._TYPE).get.convertTo[String]
      elementType.equalsIgnoreCase(GraphSONTokens.VERTEX)
    }
    case _ => false
  }

  /**
   * Check if GraphSON contains a Blueprints graph elements
   */
  private def isGraphElement(json: JsValue): Boolean = isEdge(json) | isVertex(json)

  /**
   * Get element ID from GraphSON
   */
  private def getElementId(json: JsValue, idName: String): AnyRef = json.asJsObject.fields.get(idName).getOrElse(null)

}
