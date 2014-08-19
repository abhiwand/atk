package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.intelanalytics.engine.spark.graph.TestingTitan
import com.intel.testutils.MatcherUtils._
import org.scalatest.{ FlatSpec, Matchers }
import org.mockito.Mockito._
import spray.json.JsNumber
import org.scalatest.mock.MockitoSugar

class GremlinQueryITest extends FlatSpec with Matchers with TestingTitan with MockitoSugar {

  "executeGremlinQuery" should "execute valid Gremlin queries" ignore {
    val vertex1 = titanGraph.addVertex(null)
    val vertex2 = titanGraph.addVertex(null)
    val edge = titanGraph.addEdge(null, vertex1, vertex2, "knows")

    vertex1.setProperty("name", "alice")
    vertex1.setProperty("age", 23)
    vertex2.setProperty("name", "bob")
    vertex2.setProperty("age", 27)

    val gremlinQuery = new GremlinQuery()
    val gremlinScript = """g.V("name", "alice").out("knows")"""

    val bindings = gremlinQuery.gremlinExecutor.createBindings()
    bindings.put("g", titanGraph)

    val results = gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    val vertexCount = gremlinQuery.executeGremlinQuery(titanGraph, "g.V.count()", bindings).toArray
    val edgeCount = gremlinQuery.executeGremlinQuery(titanGraph, "g.E.count()", bindings).toArray

    results.size should equal(1)
    vertexCount.size should equal(1)
    edgeCount.size should equal(1)

    vertex2 should equalsGraphSONVertex(results(0))
    vertexCount(0) should equal(JsNumber(2))
    edgeCount(0) should equal(JsNumber(1))
  }
  "executeGremlinQuery" should "throw a Runtime exception when executing invalid Gremlin" ignore {
    intercept[java.lang.RuntimeException] {
      val gremlinQuery = new GremlinQuery()
      val gremlinScript = """InvalidGremlin"""

      val bindings = gremlinQuery.gremlinExecutor.createBindings()
      bindings.put("g", titanGraph)

      gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    }
  }

}
