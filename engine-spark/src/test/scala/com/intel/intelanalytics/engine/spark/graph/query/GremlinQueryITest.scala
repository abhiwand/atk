package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.graph.TestingTitan
import com.intel.intelanalytics.security.UserPrincipal
import org.scalatest.{Matchers, FlatSpec}
import org.specs2.mock.Mockito

import scala.concurrent.ExecutionContext

class GremlinQueryITest extends FlatSpec with Matchers with TestingTitan with Mockito {

  "GremlinQuery" should "execute valid Gremlin queries" in {
    val vertex = graph.addVertex(1)
    vertex.setProperty("name", "marko")
    vertex.setProperty("age", 29)

    val invocation = mock[Invocation]
    implicit val user = mock[UserPrincipal]
    implicit val executionContext = mock[ExecutionContext]
    val gremlinPlugin = new GremlinQuery
    gremlinPlugin.configuration
    val queryArgs = new QueryArgs("g.V")
    val queryResults = gremlinPlugin.execute(invocation, queryArgs)

  }

}
