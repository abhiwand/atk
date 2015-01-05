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

package com.intel.intelanalytics.domain.schema
import com.intel.intelanalytics.domain.schema.DataTypes.DataType

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SchemaUtilTest extends FlatSpec with Matchers {

  "SchemaUtil" should "be able to resolve name conflicts when they exist" in {
    val leftColumns: List[(String, DataType)] = List(("same", DataTypes.int32), ("bar", DataTypes.int32))
    val rightColumns: List[(String, DataType)] = List(("same", DataTypes.int32), ("foo", DataTypes.string))

    val result = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)

    result.length shouldBe 4
    result(0)._1 shouldBe "same_L"
    result(1)._1 shouldBe "bar"
    result(2)._1 shouldBe "same_R"
    result(3)._1 shouldBe "foo"
  }

  it should "not do anything to resolve conflicts when they don't exist" in {
    val leftColumns: List[(String, DataType)] = List(("left", DataTypes.int32), ("bar", DataTypes.int32))
    val rightColumns: List[(String, DataType)] = List(("right", DataTypes.int32), ("foo", DataTypes.string))

    val result = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)

    result.length shouldBe 4
    result(0)._1 shouldBe "left"
    result(1)._1 shouldBe "bar"
    result(2)._1 shouldBe "right"
    result(3)._1 shouldBe "foo"
  }

  it should "handle empty column lists" in {
    val leftColumns: List[(String, DataType)] = List(("left", DataTypes.int32), ("bar", DataTypes.int32))
    val rightColumns: List[(String, DataType)] = List()

    val result = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)

    result.length shouldBe 2
    result(0)._1 shouldBe "left"
    result(1)._1 shouldBe "bar"
  }

  it should "repeatedly appending L if L already exists in the left hand side" in {
    val leftColumns: List[(String, DataType)] = List(("data", DataTypes.int32), ("data_L", DataTypes.int32))
    val rightColumns: List[(String, DataType)] = List(("data", DataTypes.int32))

    val result = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)
    result.length shouldBe 3
    result(0)._1 shouldBe "data_L_L"
    result(1)._1 shouldBe "data_L"
    result(2)._1 shouldBe "data_R"
  }

  it should "repeatedly appending L if L already exists in the right hand side" in {
    val leftColumns: List[(String, DataType)] = List(("data", DataTypes.int32))
    val rightColumns: List[(String, DataType)] = List(("data", DataTypes.int32), ("data_L", DataTypes.int32))

    val result = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)
    result.length shouldBe 3
    result(0)._1 shouldBe "data_L_L"
    result(1)._1 shouldBe "data_R"
    result(2)._1 shouldBe "data_L"
  }

  it should "repeatedly appending R if R already exists in the left hand side" in {
    val leftColumns: List[(String, DataType)] = List(("data", DataTypes.int32), ("data_R", DataTypes.int32))
    val rightColumns: List[(String, DataType)] = List(("data", DataTypes.int32))

    val result = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)
    result.length shouldBe 3
    result(0)._1 shouldBe "data_L"
    result(1)._1 shouldBe "data_R"
    result(2)._1 shouldBe "data_R_R"
  }

  "convertSchema" should "return the same data if schema does not change" in {
    val columns = List(Column("name", DataTypes.string), Column("age", DataTypes.int32))
    val data = Array("Frank", 48)

    SchemaUtil.convertSchema(new FrameSchema(columns), new FrameSchema(columns), data) should be(data)
  }

  "convertSchema" should "reorder the columns if the new schema is simply reordered" in {
    val columns = List(Column("name", DataTypes.string), Column("age", DataTypes.int32))
    val data = Array("Frank", 48)

    SchemaUtil.convertSchema(new FrameSchema(columns), new FrameSchema(columns.reverse), data) should be(data.reverse)
  }

  "convertSchema" should "add nulls where appropriate if the schema contains new columns" in {
    val oldColumns = List(Column("name", DataTypes.string), Column("age", DataTypes.int32))
    val newColumns = List(
      Column("phone_number", DataTypes.string), Column("name", DataTypes.string), Column("address", DataTypes.string), Column("age", DataTypes.int32))
    val data = Array("Frank", 48)
    val expected = Array(null, "Frank", null, 48)

    SchemaUtil.convertSchema(new FrameSchema(oldColumns), new FrameSchema(newColumns), data) should be(expected)
  }

  "convertSchema" should "convert data types when they are changed" in {
    val oldColumns = List(Column("length", DataTypes.int32), Column("width", DataTypes.int32))
    val newColumns = List(Column("length", DataTypes.float64), Column("width", DataTypes.string))
    val data = Array(15, 10)
    val expected = Array(15.0, "10")

    SchemaUtil.convertSchema(new FrameSchema(oldColumns), new FrameSchema(newColumns), data) should be(expected)
  }

  "convertSchema" should "remove data that is not found in the new schema" in {
    val oldColumns = List(Column("name", DataTypes.string), Column("age", DataTypes.int32))
    val newColumns = List(Column("name", DataTypes.string))
    val data = Array("Frank", 48)
    val expected = Array("Frank")

    SchemaUtil.convertSchema(new FrameSchema(oldColumns), new FrameSchema(newColumns), data) should be(expected)
  }

  "mergeSchema" should "keep the schema the same if schemas are identical" in {
    val schema = new FrameSchema(List(Column("name", DataTypes.string), Column("age", DataTypes.int32)))
    SchemaUtil.mergeSchema(schema, schema) should be(schema)
  }

  "mergeSchema" should "keep the ordering of the left schema if a name appears in a different order" in {
    val leftSchema = new FrameSchema(List(Column("name", DataTypes.string), Column("age", DataTypes.int32)))
    val rightSchema = new FrameSchema(List(Column("age", DataTypes.int32), Column("name", DataTypes.string)))
    SchemaUtil.mergeSchema(leftSchema, rightSchema) should be(leftSchema)
  }

  "mergeSchema" should "append columns to the left schema that are only in the right" in {
    val leftSchema = new FrameSchema(List(Column("name", DataTypes.string), Column("age", DataTypes.int32)))
    val rightSchema = new FrameSchema(List(Column("height", DataTypes.int32), Column("name", DataTypes.string)))
    val expectedSchema = new FrameSchema(List(Column("name", DataTypes.string), Column("age", DataTypes.int32), Column("height", DataTypes.int32)))
    SchemaUtil.mergeSchema(leftSchema, rightSchema) should be(expectedSchema)
  }

  "mergeSchema" should "change data types to a more general type if they are different" in {
    val leftSchema = new FrameSchema(List(
      Column("name", DataTypes.string), Column("age", DataTypes.int32), Column("height", DataTypes.float32), Column("weight", DataTypes.int32)))
    val rightSchema = new FrameSchema(List(
      Column("name", DataTypes.int32), Column("age", DataTypes.int64), Column("height", DataTypes.int64), Column("weight", DataTypes.float32)))
    val expectedSchema = new FrameSchema(List(
      Column("name", DataTypes.string), Column("age", DataTypes.int64), Column("height", DataTypes.float64), Column("weight", DataTypes.float32)))
    SchemaUtil.mergeSchema(leftSchema, rightSchema) should be(expectedSchema)
  }

}
