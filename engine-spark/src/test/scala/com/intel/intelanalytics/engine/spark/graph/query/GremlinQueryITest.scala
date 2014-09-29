package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.testutils.MatcherUtils._
import com.intel.testutils.TestingTitan
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import spray.json.JsNumber

class GremlinQueryITest extends FlatSpec with Matchers with TestingTitan with MockitoSugar with BeforeAndAfter {
  before {
    setupTitan()
  }

  after {
    cleanupTitan()
  }

  "executeGremlinQuery" should "execute valid Gremlin queries" in {
    val vertex1 = graph.addVertex(null)
    val vertex2 = graph.addVertex(null)
    val edge = graph.addEdge(null, vertex1, vertex2, "knows")

    vertex1.setProperty("name", "alice")
    vertex1.setProperty("age", 23)
    vertex2.setProperty("name", "bob")
    vertex2.setProperty("age", 27)

    val gremlinQuery = new GremlinQuery()
    val gremlinScript = """g.V("name", "alice").out("knows")"""

    val bindings = gremlinQuery.gremlinExecutor.createBindings()
    bindings.put("g", graph)

    val results = gremlinQuery.executeGremlinQuery(graph, gremlinScript, bindings).toArray
    val vertexCount = gremlinQuery.executeGremlinQuery(graph, "g.V.count()", bindings).toArray
    val edgeCount = gremlinQuery.executeGremlinQuery(graph, "g.E.count()", bindings).toArray

    results.size should equal(1)
    vertexCount.size should equal(1)
    edgeCount.size should equal(1)

    vertex2 should equalsGraphSONVertex(results(0))
    vertexCount(0) should equal(JsNumber(2))
    edgeCount(0) should equal(JsNumber(1))
  }
  "executeGremlinQuery" should "throw a Runtime exception when executing invalid Gremlin" in {
    intercept[java.lang.RuntimeException] {
      val gremlinQuery = new GremlinQuery()
      val gremlinScript = """InvalidGremlin"""

      val bindings = gremlinQuery.gremlinExecutor.createBindings()
      bindings.put("g", graph)

      gremlinQuery.executeGremlinQuery(graph, gremlinScript, bindings).toArray
    }
  }

}
