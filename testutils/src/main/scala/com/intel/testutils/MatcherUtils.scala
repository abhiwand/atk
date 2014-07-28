package com.intel.testutils

import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens
import com.tinkerpop.blueprints.{ Direction, Edge, Vertex }
import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }
import spray.json._

import scala.collection.JavaConverters._

object MatcherUtils extends Matchers {

  /**
   * Tests if two arrays of Double are equal +- tolerance.
   *
   * <pre class="stHighlight">
   * Array(0.12, 0.25) should  equalWithTolerance(Array(0.122, 0.254), 0.01)
   * </pre>
   */
  def equalWithTolerance(right: Array[Double], tolerance: Double) =
    Matcher { (left: Array[Double]) =>
      MatchResult(
        (left zip right) forall { case (a, b) => a === (b +- tolerance) },
        left + " did not equal " + right + " with tolerance " + tolerance,
        left + " equaled " + right + " with tolerance " + tolerance
      )
    }

  /**
   * Tests if the GraphSON representation of the Blueprint's vertex is valid
   *
   * <pre class="stHighlight">
   * "{"name":"marko", "age":29, "_id":10, "_type":"vertex" }" should  equalsBlueprintsVertex(vertex)
   * </pre>
   */
  def equalsGraphSONVertex(json: JsValue) =
    Matcher { (vertex: Vertex) =>
      MatchResult(matchGraphSONVertex(vertex, json),
        json + " does not equal the GraphSON representation for " + vertex,
        json + " equals the GraphSON representation for " + vertex
      )
    }

  /**
   * Tests if the GraphSON representation of the Blueprint's vertex is valid
   *
   * <pre class="stHighlight">
   * {"weight":0.5,"_id":7,"_type":"edge","_outV":1,"_inV":2,"_label":"knows"}" should  equalsBlueprintsEdge(edge)
   * </pre>
   */
  def equalsGraphSONEdge(json: JsValue) =
    Matcher { (edge: Edge) =>
      MatchResult(matchGraphSONEdge(edge, json),
        json + " does not equal the GraphSON representation for " + edge,
        json + " equals the GraphSON representation for " + edge
      )
    }

  /**
   * Returns true if JSON is a valid GraphSON representation of vertex
   */
  private def matchGraphSONVertex(vertex: Vertex, json: JsValue): Boolean = {
    getJsonFieldValue(json, GraphSONTokens._ID) === vertex.getId &&
      getJsonFieldValue(json, GraphSONTokens._TYPE) === GraphSONTokens.VERTEX &&
      (vertex.getPropertyKeys.asScala forall {
        case a => vertex.getProperty(a).toString === getJsonFieldValue(json, a).toString
      })
  }

  /**
   * Returns true if JSON is a valid GraphSON representation of edge
   */
  private def matchGraphSONEdge(edge: Edge, json: JsValue): Boolean = {
    getJsonFieldValue(json, GraphSONTokens._TYPE) === GraphSONTokens.EDGE &&
      getJsonFieldValue(json, GraphSONTokens._IN_V) === edge.getVertex(Direction.IN).getId &&
      getJsonFieldValue(json, GraphSONTokens._OUT_V) === edge.getVertex(Direction.OUT).getId &&
      getJsonFieldValue(json, GraphSONTokens._LABEL) === edge.getLabel &&
      (edge.getPropertyKeys.asScala forall {
        case a => edge.getProperty(a).toString === getJsonFieldValue(json, a).toString
      })
  }

  /**
   * Get field value from JSON object using key, and convert value to a Scala object
   */
  private def getJsonFieldValue(json: JsValue, key: String): Any = json match {
    case obj: JsObject => {
      val value = obj.fields.get(key).orNull
      value match {
        case x: JsBoolean => x.value
        case x: JsNumber => x.value
        case x: JsString => x.value
        case x => x.toString
      }
    }
    case x => x.toString()
  }

}
