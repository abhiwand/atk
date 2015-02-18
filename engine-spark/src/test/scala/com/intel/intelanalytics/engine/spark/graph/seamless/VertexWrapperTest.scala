//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, Schema, VertexSchema }
import org.apache.spark.ia.graph.VertexWrapper
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ FlatSpec, Matchers }

class VertexWrapperTest extends FlatSpec with Matchers {

  val columns = List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))

  val schema = new VertexSchema(columns, "label", null)

  "VertexWrapper" should "allow accessing underlying vertex data" in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    wrapper.vid shouldBe 1L
    wrapper.label shouldBe "l1"

    wrapper.stringValue("name") shouldBe "Bob"
    wrapper.stringValue("from") shouldBe "PDX"
    wrapper.stringValue("to") shouldBe "LAX"
    wrapper.intValue("fair") shouldBe 350

  }

  "get property value" should "raise exception when the property is not valid" in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)

    //make sure it throw exception when accessing invalid property
    intercept[IllegalArgumentException] {
      wrapper.stringValue("random")
    }
  }

  "hasProperty" should "return false if property does not exist " in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    wrapper.hasProperty("name") shouldBe true
    wrapper.hasProperty("from") shouldBe true
    wrapper.hasProperty("random_column") shouldBe false
  }

  "VertexWrapper" should "allow modifying  vertex data" ignore {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    wrapper.setValue("from", "SFO")
    wrapper.stringValue("from") shouldBe "SFO"
  }

  "VertexWrapper" should "convert to a valid GBVertex" in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    val gbVertex = wrapper.toGbVertex

    gbVertex.gbId.key should be("_vid")
    gbVertex.gbId.value should be(1L)
    gbVertex.getProperty("name").get.value shouldBe ("Bob")
    gbVertex.getProperty("from").get.value shouldBe ("PDX")
    gbVertex.getProperty("to").get.value shouldBe ("LAX")
    gbVertex.getProperty("fair").get.value shouldBe (350)
  }
}
