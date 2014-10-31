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

import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{FrameRDD, SparkFrameStorage}
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.ia.graph.{EdgeFrameRDD, VertexFrameRDD}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Vertex => GBVertex }

class ExportToTitanGraphTest extends TestingSparkContextWordSpec with Matchers with MockitoSugar  {
  val edgeColumns = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("startDate", DataTypes.string))
  val edgeSchema = new Schema(edgeColumns, edgeSchema = Some(EdgeSchema("label", "srclabel", "destlabel")))

  val employeeColumns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("employeeID", DataTypes.int64))
  val employeeSchema = new Schema(employeeColumns, Some(VertexSchema("label", null)))

  val divisionColumns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("divisionID", DataTypes.int64))
  val divisionSchema = new Schema(divisionColumns, Some(VertexSchema("label", null)))
  
  "ExportToTitanGraph" should {
    "convert the seamlessGraph vertices into a single RDD[GBVertex] object" in {
      val employees = List(
        new GenericRow(Array(1L, "employee", "Bob", 100L)),
        new GenericRow(Array(2L, "employee", "Joe", 101L)))
      val employeeRDD = sparkContext.parallelize[sql.Row](employees)
      val employeeFrameRDD = new VertexFrameRDD(employeeSchema, employeeRDD)

      val divisions = List(new GenericRow(Array(3L, "division", "development", 200L)))
      val divisionRDD = sparkContext.parallelize[sql.Row](divisions)
      val divisionFrameRDD = new VertexFrameRDD(employeeSchema, divisionRDD)
      val plugin = new ExportToTitanGraph(mock[SparkFrameStorage], mock[SparkGraphStorage])

      val gbVertices = plugin.toGBVertices(sparkContext, List(employeeFrameRDD, divisionFrameRDD))

      val values = gbVertices.collect()

      values.length should be(3)
      values(0).gbId.value should be(1L)
      values(1).gbId.value should be(2L)
      values(2).gbId.value should be(3L)
    }

    "convert the seamlessGraph edges into a single RDD[GBEdge] object" in {
      val works = List(
        new GenericRow(Array(4L, 1L, 3L,"worksIn", "10/15/2012")),
        new GenericRow(Array(5L, 2L, 3L,"worksIn", "9/01/2014")))
      val edgeRDD = sparkContext.parallelize[sql.Row](works)
      val edgeFrameRDD = new EdgeFrameRDD(edgeSchema, edgeRDD)
      val plugin = new ExportToTitanGraph(mock[SparkFrameStorage], mock[SparkGraphStorage])

      val gbEdges = plugin.toGBEdges(sparkContext, List(edgeFrameRDD))

      val values = gbEdges.collect()

      values.length should be(2)
      values(0).tailVertexGbId.value should be(1L)
      values(0).headVertexGbId.value should be(3L)
      values(1).tailVertexGbId.value should be(2L)
      values(1).headVertexGbId.value should be(3L)
    }
  }

}
