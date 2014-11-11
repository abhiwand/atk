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

import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, EdgeSchema, Schema }
import org.apache.spark.ia.graph.EdgeWrapper
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ FlatSpec, Matchers }

class EdgeWrapperTest extends FlatSpec with Matchers {

  val columns = List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("distance", DataTypes.int32))

  val schema = new Schema(columns, edgeSchema = Some(EdgeSchema("label", "srclabel", "destlabel")))

  "EdgeWrapper" should "allow access to underlying edge data" in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    wrapper.eid shouldBe 1L
    wrapper.label shouldBe "distance"

    wrapper.srcVertexId shouldBe 2L
    wrapper.destVertexId shouldBe 3L
    wrapper.intValue("distance") shouldBe 500
  }

  "get property value" should "raise exception when the property is not valid" in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)

    //make sure it throw exception when accessing invalid property
    intercept[IllegalArgumentException] {
      wrapper.stringValue("random")
    }
  }

  "hasProperty" should "return false if property does not exist " in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    wrapper.hasProperty("_src_vid") shouldBe true
    wrapper.hasProperty("distance") shouldBe true
    wrapper.hasProperty("random_column") shouldBe false
  }

  "EdgeWrapper" should "allow modifying edge data" ignore {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    wrapper.setValue("distance", 350)
    wrapper.intValue("distance") shouldBe 350
  }

  "EdgeWrapper" should "create a GBEdge Type" in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    val gbEdge = wrapper.toGbEdge
    gbEdge.label should be("label")
    gbEdge.getProperty("distance").get.value should be(500)
    gbEdge.getProperty("_eid").get.value should be(1)
    gbEdge.tailVertexGbId.key should be("_vid")
    gbEdge.headVertexGbId.key should be("_vid")
    gbEdge.tailVertexGbId.value should be(2L)
    gbEdge.headVertexGbId.value should be(3L)
  }
}
