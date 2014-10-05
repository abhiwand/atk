package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.testutils.MatcherUtils._
import com.intel.testutils.TestingTitan
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import spray.json.JsNumber

class GremlinQueryITest extends FlatSpec with Matchers with TestingTitan with MockitoSugar with BeforeAndAfter {
  before {
    setupTitan()
  }

  after {
    cleanupTitan()
  }

  "executeGremlinQuery" should "execute valid Gremlin queries" in {
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
  "executeGremlinQuery" should "throw a Runtime exception when executing invalid Gremlin" in {
    intercept[java.lang.RuntimeException] {
      val gremlinQuery = new GremlinQuery()
      val gremlinScript = """InvalidGremlin"""

      val bindings = gremlinQuery.gremlinExecutor.createBindings()
      bindings.put("g", titanGraph)

      gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    }
  }

}
