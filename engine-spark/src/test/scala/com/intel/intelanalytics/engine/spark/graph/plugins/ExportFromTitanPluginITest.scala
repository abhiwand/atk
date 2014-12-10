package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.engine.plugin.Call
import org.scalatest.{ FlatSpec, Matchers }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.intel.intelanalytics.domain.schema.{ Column, VertexSchema }
import com.intel.intelanalytics.domain.schema.DataTypes.{ string, int64 }

class ExportFromTitanPluginITest extends FlatSpec with Matchers with MockitoSugar {
  implicit val call = Call(null)

  "createVertexFrames" should "vertex frame by label" in {
    val graphs = mock[SparkGraphStorage]
    val graphId = 1
    ExportFromTitanPlugin.createVertexFrames(graphs, graphId, List("user", "movie"))

    verify(graphs).defineVertexType(graphId, VertexSchema(List(Column("_vid", int64), Column("_label", string)), label = "user", idColumnName = None))
    verify(graphs).defineVertexType(graphId, VertexSchema(List(Column("_vid", int64), Column("_label", string)), label = "movie", idColumnName = None))
  }
}
