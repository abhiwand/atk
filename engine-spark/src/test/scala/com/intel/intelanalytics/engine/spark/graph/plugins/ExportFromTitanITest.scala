package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.{ FlatSpec, BeforeAndAfter, Matchers }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage

import org.scalatest.mock.MockitoSugar
import com.intel.graphbuilder.elements.{ Property, GBVertex }
import org.mockito.Mockito._
import scala.Some
import com.intel.intelanalytics.domain.schema.VertexSchema

class ExportFromTitanITest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  "createVertexFrames" should "vertex frame by label" in {
    val graphs = mock[SparkGraphStorage]
    val vertex1 = GBVertex(1, Property("key", 1), Set(Property("_label", "user")))
    val vertex2 = GBVertex(2, Property("key", 2), Set(Property("_label", "movie")))
    val verticesRdd = sparkContext.parallelize(List(vertex1, vertex2))

    val graphId = 1
    ExportFromTitanGraph.createVertexFrames(graphs, graphId, verticesRdd)

    verify(graphs).defineVertexType(graphId, VertexSchema("user", None))
    verify(graphs).defineVertexType(graphId, VertexSchema("movie", None))
  }
}
