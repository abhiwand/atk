package com.intel.intelanalytics.service.v1.decorators

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.intel.intelanalytics.domain.graph.Graph
import org.joda.time.DateTime

class GraphDecoratorSpec extends FlatSpec with Matchers {

  val uri = "http://www.example.com/graphs"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val graph = new Graph(1, "name", None, "storage", 1L, new DateTime, new DateTime)

  "GraphDecorator" should "be able to decorate a graph" in {
    val decoratedGraph = GraphDecorator.decorateEntity(null, relLinks, graph)
    decoratedGraph.id should be(1)
    decoratedGraph.name should be("name")
    decoratedGraph.links.head.uri should be("http://www.example.com/graphs")
  }

  it should "set the correct URL in decorating a list of graphs" in {
    val graphHeaders = GraphDecorator.decorateForIndex(uri, Seq(graph))
    val graphHeader = graphHeaders.toList.head
    graphHeader.url should be("http://www.example.com/graphs/1")
  }
}
