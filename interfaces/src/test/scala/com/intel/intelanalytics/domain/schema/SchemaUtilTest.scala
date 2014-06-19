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

class SchemaUtilTest extends FlatSpec with Matchers{
   "convertSchema" should "return the same data if schema does not change" in {
       val columns:List[(String,DataType)] = List(("name", DataTypes.string), ("age", DataTypes.int32))
       val data = Array("Frank",48)

       SchemaUtil.convertSchema(Schema(columns),Schema(columns), data) should be(data)
   }

  "convertSchema" should "reorder the columns if the new schema is simply reordered" in {
    val columns:List[(String,DataType)] = List(("name", DataTypes.string), ("age", DataTypes.int32))
    val data = Array("Frank",48)

    SchemaUtil.convertSchema(Schema(columns),Schema(columns.reverse), data) should be(data.reverse)
  }

  "convertSchema" should "add nulls where appropriate if the schema contains new columns" in {
    val oldColumns:List[(String,DataType)] = List(("name", DataTypes.string), ("age", DataTypes.int32))
    val newColumns:List[(String,DataType)] = List(
      ("phone_number", DataTypes.string), ("name", DataTypes.string), ("address", DataTypes.string), ("age", DataTypes.int32))
    val data = Array("Frank",48)
    val expected = Array(null, "Frank", null, 48)

    SchemaUtil.convertSchema(Schema(oldColumns), Schema(newColumns), data) should be(expected)
  }

  "convertSchema" should "convert data types when they are changed" in {
    val oldColumns:List[(String,DataType)] = List(("length", DataTypes.int32), ("width", DataTypes.int32))
    val newColumns:List[(String,DataType)] = List(("length", DataTypes.float64), ("width", DataTypes.string))
    val data = Array(15,10)
    val expected = Array(15.0, "10")

    SchemaUtil.convertSchema(Schema(oldColumns), Schema(newColumns), data) should be(expected)
  }

  "convertSchema" should "remove data that is not found in the new schema" in {
    val oldColumns:List[(String,DataType)] = List(("name", DataTypes.string), ("age", DataTypes.int32))
    val newColumns:List[(String,DataType)] = List(("name", DataTypes.string))
    val data = Array("Frank",48)
    val expected = Array("Frank")

    SchemaUtil.convertSchema(Schema(oldColumns), Schema(newColumns), data) should be(expected)
  }

  "mergeSchema" should "keep the schema the same if schemas are identical" in {
    val schema = Schema(List(("name", DataTypes.string), ("age", DataTypes.int32)))
    SchemaUtil.mergeSchema(schema,schema) should be (schema)
  }

  "mergeSchema" should "keep the ordering of the left schema if a name appears in a different order" in {
    val leftSchema = Schema(List(("name", DataTypes.string), ("age", DataTypes.int32)))
    val rightSchema = Schema(List(("age", DataTypes.int32), ("name", DataTypes.string)))
    SchemaUtil.mergeSchema(leftSchema,rightSchema) should be (leftSchema)
  }

  "mergeSchema" should "append columns to the left schema that are only in the right" in {
    val leftSchema = Schema(List(("name", DataTypes.string), ("age", DataTypes.int32)))
    val rightSchema = Schema(List(("height", DataTypes.int32),("name", DataTypes.string)))
    val expectedSchema = Schema(List(("name", DataTypes.string), ("age", DataTypes.int32),("height", DataTypes.int32)))
    SchemaUtil.mergeSchema(leftSchema,rightSchema) should be (expectedSchema)
  }

  "mergeSchema" should "change data types to a more general type if they are different" in {
    val leftSchema = Schema(List(
      ("name", DataTypes.string), ("age", DataTypes.int32), ("height", DataTypes.float32), ("weight", DataTypes.int32)))
    val rightSchema = Schema(List(
      ("name", DataTypes.int32), ("age", DataTypes.int64), ("height", DataTypes.int64), ("weight", DataTypes.float32)))
    val expectedSchema = Schema(List(
      ("name", DataTypes.string), ("age", DataTypes.int64), ("height", DataTypes.float64), ("weight", DataTypes.float32)))
    SchemaUtil.mergeSchema(leftSchema,rightSchema) should be (expectedSchema)
  }

}
