package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.intelanalytics.engine.spark.graph.TestingTitan
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens
import com.tinkerpop.blueprints.{ Element, Vertex, Edge }
import org.scalatest.{ FlatSpec, Matchers }
import spray.json._

class GremlinJsonProtocolTest extends FlatSpec with Matchers with TestingTitan {

  import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._

  "GraphSONFormat" should "convert Blueprint's vertex into GraphSON" in {
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val vertex = graph.addVertex(1)
    vertex.setProperty("name", "marko")
    vertex.setProperty("age", 29)

    val json = vertex.asInstanceOf[Element].toJson
    val jsonFields = json.asJsObject.fields

    jsonFields should contain key GraphSONTokens._ID
    jsonFields should contain(GraphSONTokens._TYPE, JsString(GraphSONTokens.VERTEX))
    jsonFields should contain("name", JsString("marko"))
    jsonFields should contain("age", JsNumber(29))
  }
  it should "convert GraphSON into a Blueprint's vertex" in {
    val vertexJson1 = """ {"name":"marko", "age":29, "_id":10, "_type":"vertex" }"""
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val vertex = JsonParser(vertexJson1).convertTo[Element]

    vertex.isInstanceOf[Vertex] should be(true)
    vertex.getProperty[String]("name") should equal("marko")
    vertex.getProperty[Int]("age") should equal(29)
  }

  it should "convert a Blueprint's edge into GraphSON" in {
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val vertex1 = graph.addVertex(1)
    val vertex2 = graph.addVertex(2)
    val edge = graph.addEdge(3, vertex1, vertex2, "test")
    edge.setProperty("weight", 0.5f)

    val json = edge.asInstanceOf[Element].toJson
    val jsonFields = json.asJsObject.fields

    jsonFields should contain key GraphSONTokens._ID
    jsonFields should contain(GraphSONTokens._LABEL, JsString("test"))
    jsonFields should contain key GraphSONTokens._IN_V
    jsonFields should contain key GraphSONTokens._OUT_V
    jsonFields should contain("weight", JsNumber(0.5f))
  }

  it should "convert GraphSON into a Blueprint's edge" in {
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val vertex1 = graph.addVertex(1)
    val vertex2 = graph.addVertex(2)

    val edgeJson1 = s"""{"weight": 10, "_label":"test", "_outV":${vertex1.getId}, "_inV":${vertex2.getId}, "_type":"edge"}"""
    val edge = JsonParser(edgeJson1).convertTo[Element]

    edge.isInstanceOf[Edge] should be(true)
    edge.getProperty[Float]("weight") should equal(10)
    edge.asInstanceOf[Edge].getLabel() should be("test")

  }
  it should "throw a deserialization exception when converting invalid GraphSON" in {
    intercept[spray.json.DeserializationException] {
      implicit val graphSONFormat = new GraphSONFormat(graph)
      val json = "[1, 2, 3]"
      JsonParser(json).convertTo[Element]
    }
  }
  /*"GremlinFormat" should "dgas" in {
    implicit val gremlinJsonFormat = new GremlinJsonFormat(graph)
    val vertex = graph.addVertex(1)
    vertex.setProperty("name", "marko")
    vertex.setProperty("age", 29)

    val json = vertex.asInstanceOf[Element].toJson
  } */
}
