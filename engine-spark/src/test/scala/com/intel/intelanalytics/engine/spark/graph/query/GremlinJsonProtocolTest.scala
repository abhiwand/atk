package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.intelanalytics.engine.spark.graph.TestingTitan
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens
import com.tinkerpop.blueprints.util.wrappers.id.IdVertex
import com.tinkerpop.blueprints.{ Edge, Element, Vertex }
import com.tinkerpop.pipes.util.structures.Row
import org.scalatest.{ FlatSpec, Matchers }
import spray.json._

import scala.collection.JavaConversions._

class GremlinJsonProtocolTest extends FlatSpec with Matchers with TestingTitan {

  import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._

  "GraphSONFormat" should "convert Blueprint's vertex into GraphSON" in {
    implicit val graphSONFormat = new GraphSONFormat(graph)
    val vertex = graph.addVertex(1)
    vertex.setProperty("name", "marko")
    vertex.setProperty("age", 29)

    val json = vertex.asInstanceOf[Element].toJson
    val jsonFields = json.asJsObject.fields

    jsonFields should contain(GraphSONTokens._ID, JsNumber(1))
    jsonFields should contain(GraphSONTokens._TYPE, JsString(GraphSONTokens.VERTEX))
    jsonFields should contain("name", JsString("marko"))
    jsonFields should contain("age", JsNumber(29))
  }

  it should "convert GraphSON into a Blueprint's vertex" in {
    implicit val graphSONFormat = new GraphSONFormat(graph)

    val vertexJson1 = """{"name":"marko", "age":29, "_id":10, "_type":"vertex" }"""
    val vertex = JsonParser(vertexJson1).convertTo[Element]

    vertex.isInstanceOf[Vertex] should be(true)
    vertex.getId() should equal(10)
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
    jsonFields should contain(GraphSONTokens._OUT_V, JsNumber(1))
    jsonFields should contain(GraphSONTokens._IN_V, JsNumber(2))
    jsonFields should contain("weight", JsNumber(0.5f))
  }

  it should "convert GraphSON into a Blueprint's edge" in {
    implicit val graphSONFormat = new GraphSONFormat(titanGraph)
    val vertex1 = titanGraph.addVertex(null)
    val vertex2 = titanGraph.addVertex(null)

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

  "BlueprintsRowFormat" should "convert Blueprint's row into a JSON" in {
    val rowMap = Map("col1" -> "val1", "col2" -> "val2")
    val row = new Row(rowMap.values.toList, rowMap.keys.toList)

    val json = row.toJson
    val jsonFields = json.asJsObject.fields

    jsonFields.keySet should contain theSameElementsAs (rowMap.keySet)
    jsonFields.values.toList should contain theSameElementsAs (List(JsString("val1"), JsString("val2")))

  }

  it should "convert a JsObject to a Blueprint's row" in {
    val json = Map("col1" -> 1, "col2" -> 2).toJson
    val jsonFields = json.asJsObject.fields

    val row = json.convertTo[Row[Int]]

    row.getColumnNames should contain theSameElementsAs (jsonFields.keySet)
    row.getColumnNames.map(row.getColumn(_)) should contain theSameElementsAs (List(1, 2))
  }

  it should "throw a deserialization exception when JSON is not a valid JsObject" in {
    intercept[spray.json.DeserializationException] {
      val json = """["test1", "test2"]"""
      JsonParser(json).convertTo[Row[String]]
    }
  }

  "GremlinJsonProtocol" should "return true if GraphSON refers to a graph element" in {
    val vertexJson1 = """{"name":"saturn",  "_id":10, "_type":"vertex" }"""
    val edgeJson1 = """{"name":"saturn",  "_id":10, "_type":"vertex" }"""
  }

}
