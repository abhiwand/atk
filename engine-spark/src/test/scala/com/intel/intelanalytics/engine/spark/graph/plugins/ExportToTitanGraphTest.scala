package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{FrameRDD, SparkFrameStorage}
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.ia.graph.VertexFrameRDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar

class ExportToTitanGraphTest extends TestingSparkContextWordSpec with Matchers with MockitoSugar  {
//  val edgeColumns = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("distance", DataTypes.int32))
//  val edgeSchema = new Schema(edgeColumns, edgeSchema = Some(EdgeSchema("label", "srclabel", "destlabel")))

  val employeeColumns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("employeeID", DataTypes.int64))
  val employeeSchema = new Schema(employeeColumns, Some(VertexSchema("label", null)))

  val divisionColumns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("divisionID", DataTypes.int64))
  val divisionSchema = new Schema(divisionColumns, Some(VertexSchema("label", null)))
  
  "ExportToTitanGraph" should {
    "convert the seamlessGraph vertices into a single RDD[GBEdge] object" in {
      val employees = List(
        new GenericRow(Array(1L, "employee", "Bob", 100L)),
        new GenericRow(Array(2L, "employee", "Joe", 101L)))
      val employeeRDD = sparkContext.parallelize[sql.Row](employees)
      val employeeFrameRDD = new VertexFrameRDD(employeeSchema, employeeRDD)

      val divisions = List(new GenericRow(Array(1L, "division", "development", 200L)))
      val divisionRDD = sparkContext.parallelize[sql.Row](divisions)
      val divisionFrameRDD = new VertexFrameRDD(employeeSchema, divisionRDD)
      val plugin = new ExportToTitanGraph(mock[SparkFrameStorage], mock[SparkGraphStorage])

      val gbVertices = plugin.toGBVertices(sparkContext, List(employeeFrameRDD, divisionFrameRDD))

      gbVertices.count() should be(3)
    }
  }

}
