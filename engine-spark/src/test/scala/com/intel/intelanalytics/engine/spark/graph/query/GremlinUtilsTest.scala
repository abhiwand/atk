package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.testutils.TestingTitan
import com.intel.testutils.MatcherUtils._
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.blueprints.{ Edge, Element, Vertex }
import com.tinkerpop.pipes.util.structures.Row
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import spray.json._

import scala.collection.JavaConversions._

class GremlinUtilsTest extends FlatSpec with Matchers with TestingTitan with BeforeAndAfter {

  import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._

  before {
    setupTitan()
  }

  after {
    cleanupTitan()
  }
  implicit val graphSONFormat = new GraphSONFormat(titanIdGraph)

  "serializeGremlinToJson" should "serialize a Blueprint's vertex into GraphSON" in {
    val vertex = titanIdGraph.addVertex(1)
    vertex.setProperty("name", "marko")
    vertex.setProperty("age", 29)

    val json = GremlinUtils.serializeGremlinToJson[Element](titanIdGraph, vertex).asJsObject
    vertex should equalsGraphSONVertex(json)
  }

  "serializeGremlinToJson" should "serialize a Blueprint's edge into GraphSON" in {
    val vertex1 = titanIdGraph.addVertex(1)
    val vertex2 = titanIdGraph.addVertex(2)
    val edge = titanIdGraph.addEdge(3, vertex1, vertex2, "test")
    edge.setProperty("weight", 0.5f)

    val json = GremlinUtils.serializeGremlinToJson[Element](titanIdGraph, edge)
    edge should equalsGraphSONEdge(json)
  }

  "serializeGremlinToJson" should "serialize a Blueprint's row into a JSON map" in {
    val rowMap = Map("col1" -> "val1", "col2" -> "val2")
    val row = new Row(rowMap.values.toList, rowMap.keys.toList)

    val json = GremlinUtils.serializeGremlinToJson(titanIdGraph, row).asJsObject
    val jsonFields = json.fields
    jsonFields.keySet should contain theSameElementsAs (rowMap.keySet)
    jsonFields.values.toList should contain theSameElementsAs (List(JsString("val1"), JsString("val2")))
  }

  "deserializeJsonToGremlin" should "deserialize GraphSON into a Blueprint's vertex" in {
    val json = JsonParser("""{"name":"marko", "age":29, "_id":10, "_type":"vertex" }""")
    val vertex = GremlinUtils.deserializeJsonToGremlin[Element](titanIdGraph, json)

    vertex.asInstanceOf[Vertex] should equalsGraphSONVertex(json)
  }

  "deserializeJsonToGremlin" should "deserialize GraphSON into a Blueprint's edge" in {
    val vertex1 = titanGraph.addVertex(null)
    val vertex2 = titanGraph.addVertex(null)

    val json = JsonParser(s"""{"weight": 10, "_label":"test", "_outV":${vertex1.getId}, "_inV":${vertex2.getId}, "_type":"edge"}""")
    val edge = GremlinUtils.deserializeJsonToGremlin[Element](titanGraph, json)

    edge.asInstanceOf[Edge] should equalsGraphSONEdge(json)
  }

  "deserializeJsonToGremlin" should "deserialize a JSON map to a Blueprint's row" in {
    val json = Map("col1" -> 1, "col2" -> 2).toJson
    val jsonFields = json.asJsObject.fields

    val row = GremlinUtils.deserializeJsonToGremlin[Row[Int]](titanIdGraph, json)

    row.getColumnNames should contain theSameElementsAs (jsonFields.keySet)
    row.getColumnNames.map(row.getColumn(_)) should contain theSameElementsAs (List(1, 2))
  }

  "getGraphSONMode" should "return the Blueprint's GraphSON mode for supported modes" in {
    GremlinUtils.getGraphSONMode("normal") should be(GraphSONMode.NORMAL)
    GremlinUtils.getGraphSONMode("extended") should be(GraphSONMode.EXTENDED)
    GremlinUtils.getGraphSONMode("compact") should be(GraphSONMode.COMPACT)
  }

  "getGraphSONMode" should "throw an IllegalArgumentException for unsupported modes" in {
    intercept[java.lang.IllegalArgumentException] {
      GremlinUtils.getGraphSONMode("unsupported")
    }
  }
}
