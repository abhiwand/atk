//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.testutils.TestingTitan
import com.intel.testutils.MatcherUtils._
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.blueprints.{ Edge, Element, Vertex }
import com.tinkerpop.pipes.util.structures.Row
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import spray.json._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class GremlinUtilsTest extends FlatSpec with Matchers with TestingTitan with BeforeAndAfter {
  import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._

  before {
    setupTitan()
    // Create schema before setting properties -- Needed in Titan 0.5.4+
    val graphManager = titanGraph.getManagementSystem()
    graphManager.makePropertyKey("name").dataType(classOf[String]).make()
    graphManager.makePropertyKey("age").dataType(classOf[Integer]).make()
    graphManager.makePropertyKey("weight").dataType(classOf[Integer]).make()
    graphManager.makeEdgeLabel("test").make()
    graphManager.commit()
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
    edge.setProperty("weight", 2)

    val json = GremlinUtils.serializeGremlinToJson[Element](titanIdGraph, edge)
    edge should equalsGraphSONEdge(json)
  }

  "serializeGremlinToJson" should "serialize a Blueprint's row into a JSON map" in {
    import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._
    val rowMap = Map("col1" -> "val1", "col2" -> "val2")
    val row = new Row(rowMap.values.toList, rowMap.keys.toList)

    val json = GremlinUtils.serializeGremlinToJson(titanIdGraph, row).asJsObject
    val jsonFields = json.fields
    jsonFields.keySet should contain theSameElementsAs (rowMap.keySet)
    jsonFields.values.toList should contain theSameElementsAs (List(JsString("val1"), JsString("val2")))
  }
  "serializeGremlinToJson" should "serialize Java collections to JSON" in {
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    val javaSet = Array(1, 2, 3).toSet.asJava
    val javaList = Array("Alice", "Bob", "Charles").toList.asJava

    val jsonSet = GremlinUtils.serializeGremlinToJson(titanIdGraph, javaSet)
    val jsonList = GremlinUtils.serializeGremlinToJson(titanIdGraph, javaList)

    jsonSet.convertTo[java.util.Set[Int]] should contain theSameElementsAs (javaSet)
    jsonList.convertTo[java.util.List[String]] should contain theSameElementsAs (javaList)
  }
  "serializeGremlinToJson" should "serialize Java maps to JSON" in {
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    val javaHashMap = new java.util.HashMap[String, Int]()
    javaHashMap.put("Alice", 29)
    javaHashMap.put("Bob", 45)
    javaHashMap.put("Jason", 56)

    val jsonMap = GremlinUtils.serializeGremlinToJson(titanIdGraph, javaHashMap)
    val javaJsonToHashMap = jsonMap.convertTo[java.util.HashMap[String, Int]]

    javaJsonToHashMap.keySet() should contain theSameElementsAs (javaHashMap.keySet())
    javaJsonToHashMap.values() should contain theSameElementsAs (javaHashMap.values())
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
    import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._
    val json = Map("col1" -> 1, "col2" -> 2).toJson
    val jsonFields = json.asJsObject.fields

    val row = GremlinUtils.deserializeJsonToGremlin[Row[Int]](titanIdGraph, json)

    row.getColumnNames should contain theSameElementsAs (jsonFields.keySet)
    row.getColumnNames.map(row.getColumn(_)) should contain theSameElementsAs (List(1, 2))
  }

  "deserializeJsonToGremlin" should "deserialize a JSON map to a Java Map" in {
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    val json = Map("weight1" -> 1, "weight2" -> 4).toJson
    val jsonFields = json.asJsObject.fields

    val javaHashMap = GremlinUtils.deserializeJsonToGremlin[java.util.Map[String, Double]](titanIdGraph, json)

    javaHashMap.keySet() should contain theSameElementsAs (javaHashMap.keySet())
    javaHashMap.values() should contain theSameElementsAs (javaHashMap.values())
  }

  "deserializeJsonToGremlin" should "deserialize a JSON array to a Java collection" in {
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    val jsonSet = Array(1, 2, 3).toSet.toJson
    val jsonList = Array("Alice", "Bob", "Charles").toList.toJson

    val javaSet = GremlinUtils.deserializeJsonToGremlin[java.util.Set[Int]](titanIdGraph, jsonSet)
    val javaList = GremlinUtils.deserializeJsonToGremlin[java.util.List[String]](titanIdGraph, jsonList)

    javaSet should contain theSameElementsAs (Array(1, 2, 3))
    javaList should contain theSameElementsAs (Array("Alice", "Bob", "Charles"))
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
