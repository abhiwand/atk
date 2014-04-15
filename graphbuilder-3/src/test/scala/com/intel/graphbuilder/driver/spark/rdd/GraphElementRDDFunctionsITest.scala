package com.intel.graphbuilder.driver.spark.rdd

import org.specs2.mutable.Specification
import com.intel.graphbuilder.testutils.TestingSparkContext
import com.intel.graphbuilder.elements._
import GraphBuilderRDDImplicits._
import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.elements.Edge

class GraphElementRDDFunctionsITest extends Specification {

  "GraphElementRDDFunctions" should {

    // A lot of tests are being grouped together here because it
    // is somewhat expensive to spin up a testing SparkContext
    "pass integration test" in new TestingSparkContext {

      val edge1 = new Edge(Property("gbId", 1L),Property("gbId", 2L), "myLabel", List(Property("key", "value")))
      val edge2 = new Edge(Property("gbId", 2L),Property("gbId", 3L), "myLabel", List(Property("key", "value")))

      val vertex = new Vertex(Property("gbId", 2L), Nil)

      val graphElements = sc.parallelize(List[GraphElement](edge1, edge2, vertex))

      graphElements.filterEdges().count() mustEqual 2
      graphElements.filterVertices().count() mustEqual 1
    }
  }
}


