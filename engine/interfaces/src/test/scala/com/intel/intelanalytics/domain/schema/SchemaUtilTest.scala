/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.domain.schema
import com.intel.intelanalytics.domain.schema.DataTypes.DataType

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SchemaUtilTest extends FlatSpec with Matchers {

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
