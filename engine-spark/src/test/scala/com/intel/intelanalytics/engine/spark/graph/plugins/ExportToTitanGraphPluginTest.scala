//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.graph.plugins

import java.util

import com.intel.graphbuilder.driver.spark.titan.GraphBuilderConfig
import com.intel.graphbuilder.parser.InputSchema
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.graph.{ GraphBuilderConfigFactory, TestingTitanWithSparkWordSpec, TestingTitan, SparkGraphStorage }
import com.intel.testutils.{ TestingSparkContextFlatSpec, TestingSparkContextWordSpec }
import com.tinkerpop.blueprints.Direction
import org.apache.spark.ia.graph.{ EdgeFrameRDD, VertexFrameRDD }
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.joda.time.DateTime
import org.scalatest.Matchers
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import scala.collection.JavaConversions._

class ExportToTitanGraphPluginTest extends TestingTitanWithSparkWordSpec with Matchers with MockitoSugar {
  val edgeColumns = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("startDate", DataTypes.string))
  val edgeSchema = new Schema(edgeColumns, edgeSchema = Some(EdgeSchema("worksUnder", "srclabel", "destlabel")))

  val employeeColumns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("employee", DataTypes.int64))
  val employeeSchema = new Schema(employeeColumns, Some(VertexSchema("employee", null)))

  val divisionColumns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("divisionID", DataTypes.int64))
  val divisionSchema = new Schema(divisionColumns, Some(VertexSchema("division", null)))

  "ExportToTitanGraph" should {
    "create an expected graphbuilder config " in {
      val plugin = new ExportToTitanGraphPlugin(mock[SparkFrameStorage], mock[SparkGraphStorage])
      val config = plugin.createGraphBuilderConfig("graphName")
      config.titanConfig.getProperty("storage.tablename").toString should include("graphName")
      config.append should be(false)
      config.edgeRules.size should be(0)
      config.vertexRules.size should be(0)
    }

    "load a titan graph from an Edge and vertex RDD" in {
      val employees = List(
        new GenericRow(Array(1L, "employee", "Bob", 100L)),
        new GenericRow(Array(2L, "employee", "Joe", 101L)))
      val employeeRDD = sparkContext.parallelize[sql.Row](employees)

      val employeeFrameRDD = new VertexFrameRDD(employeeSchema, employeeRDD)

      val divisions = List(new GenericRow(Array(3L, "division", "development", 200L)))
      val divisionRDD = sparkContext.parallelize[sql.Row](divisions)
      val divisionFrameRDD = new VertexFrameRDD(employeeSchema, divisionRDD)

      val vertexFrame = employeeFrameRDD.toGbVertexRDD union divisionFrameRDD.toGbVertexRDD
      val works = List(
        new GenericRow(Array(4L, 1L, 3L, "worksIn", "10/15/2012")),
        new GenericRow(Array(5L, 2L, 3L, "worksIn", "9/01/2014")))

      val edgeRDD = sparkContext.parallelize[sql.Row](works)
      val edgeFrameRDD = new EdgeFrameRDD(edgeSchema, edgeRDD)

      val edgeFrame = edgeFrameRDD.toGbEdgeRDD
      val edgeTaken = edgeFrame.take(10)

      val plugin = new ExportToTitanGraphPlugin(mock[SparkFrameStorage], mock[SparkGraphStorage])
      val config = new GraphBuilderConfig(new InputSchema(List()),
        List(),
        List(),
        this.titanConfig)
      plugin.loadTitanGraph(config, vertexFrame, edgeFrame)

      this.titanGraph.getEdges().size should be(2)
      this.titanGraph.getVertices.size should be(3)

      val bobVertex = this.titanGraph.getVertices("_vid", 1l).iterator().next()
      bobVertex.getProperty[String]("name") should be("Bob")
      val bobsDivisionIterator = bobVertex.getVertices(Direction.OUT)
      bobsDivisionIterator.size should be(1)

      val bobsDivision = bobsDivisionIterator.iterator().next()
      bobsDivision.getProperty[String]("name") should be("development")

      val bobEdges = bobVertex.getEdges(Direction.OUT)

      bobEdges.size should be(1)
    }

    "unallowed titan naming elements will throw proper exceptions" in {
      intercept[IllegalArgumentException] {
        val edgeColumns1 = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("label1", DataTypes.string), Column("label3", DataTypes.string))
        val edgeSchema1 = new Schema(edgeColumns1, edgeSchema = Some(EdgeSchema("label1", "srclabel", "destlabel")))
        val frame1 = new DataFrame(1, "name", edgeSchema1, 0L, new DateTime, Some(new DateTime))

        val edgeColumns2 = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("label2", DataTypes.string))
        val edgeSchema2 = new Schema(edgeColumns2, edgeSchema = Some(EdgeSchema("label2", "srclabel", "destlabel")))
        val frame2 = new DataFrame(1, "name", edgeSchema2, 0L, new DateTime,Some(new DateTime))

        val edgeColumns3 = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("startDate", DataTypes.string))
        val edgeSchema3 = new Schema(edgeColumns3, edgeSchema = Some(EdgeSchema("label3", "srclabel", "destlabel")))
        val frame3 = new DataFrame(1, "name", edgeSchema3, 0L, new DateTime, Some(new DateTime))

        val plugin: ExportToTitanGraphPlugin = new ExportToTitanGraphPlugin(mock[SparkFrameStorage], mock[SparkGraphStorage])
        plugin.validateLabelNames(List(frame1, frame2, frame3), List("label1", "label2", "label3"))
      }
    }
    "no exception thrown if titan naming elements are valid" in {
      val frame1 = new DataFrame(1, "name", edgeSchema, 0L, new DateTime, Some(new DateTime))

      val edgeColumns2 = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("startDate", DataTypes.string))
      val edgeSchema2 = new Schema(edgeColumns2, edgeSchema = Some(EdgeSchema("label1", "srclabel", "destlabel")))
      val frame2 = new DataFrame(1, "name", edgeSchema2, 0L, new DateTime, Some(new DateTime))

      val plugin: ExportToTitanGraphPlugin = new ExportToTitanGraphPlugin(mock[SparkFrameStorage], mock[SparkGraphStorage])
      plugin.validateLabelNames(List(frame1, frame2), List("notalabel1", "label2", "label3"))

    }
  }
}
