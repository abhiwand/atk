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

package com.intel.intelanalytics.engine.spark.graph.seamless

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.testutils.{ TestingSparkContextFlatSpec, TestingSparkContext }
import com.intel.intelanalytics.engine.spark.graph.plugins.FilterVerticesFunctions
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, Schema, DataTypes, Column }
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRDD
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.schema.EdgeSchema
import com.intel.intelanalytics.domain.schema.Schema
import scala.Some
import com.intel.intelanalytics.domain.schema.Column

class FilterVerticesTest extends TestingSparkContextFlatSpec with Matchers {

  "dropDanglingEdgesFromEdgeRdd" should "return a edge rdd with dangling edge dropped" in {
    val edgeArray = Array(Array(1, 11, 21, "like", 100), Array(2, 12, 22, "like", 80), Array(3, 13, 23, "like", 90), Array(4, 14, 24, "like", 5))
    val edgeRdd = sparkContext.parallelize(edgeArray)

    val columns = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("distance", DataTypes.int32))
    val schema = new EdgeSchema(columns, "label", "srclabel", "destlabel")
    val edgeLegacyRdd = new LegacyFrameRDD(schema, edgeRdd)

    val vertexArray = Array((5, Array(5, "Jack")), (13, Array(13, "Doris")))

    val vertexRdd = sparkContext.parallelize(vertexArray).asInstanceOf[RDD[(Any, Row)]]

    val remainingEdges = FilterVerticesFunctions.dropDanglingEdgesFromEdgeRdd(edgeLegacyRdd, 1, vertexRdd)
    val data = remainingEdges.collect().sortWith { case (row1, row2) => row1(0).asInstanceOf[Int] <= row2(0).asInstanceOf[Int] }
    data shouldBe Array(Array(1, 11, 21, "like", 100), Array(2, 12, 22, "like", 80), Array(4, 14, 24, "like", 5))
  }
}
