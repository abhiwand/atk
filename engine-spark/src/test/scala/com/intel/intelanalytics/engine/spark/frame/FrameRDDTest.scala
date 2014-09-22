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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql.{ SQLContext, SchemaRDD }
import org.apache.spark.sql.catalyst.types.{ StringType, IntegerType }
import org.scalatest.Matchers

/**
 * Sample Class used for a test case below
 * @param num  a number
 * @param name a string representation of that number
 */
case class TestCase(num: Int, name: String)

/**
 * Unit Tests for the FrameRDD class
 */
class FrameRDDTest extends TestingSparkContextFlatSpec with Matchers {
  "FrameRDD" should "be convertible into a SchemaRDD" in {
    val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
    val schema = new Schema(List(("num", DataTypes.int32), ("name", DataTypes.string)))
    val rdd = new FrameRDD(schema, rows)
    val schemaRDD = rdd.toSchemaRDD()

    schemaRDD.getClass should be(classOf[SchemaRDD])
  }

  "FrameRDD" should "create an appropriate StructType from frames Schema" in {
    val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
    val schema = new Schema(List(("num", DataTypes.int32), ("name", DataTypes.string)))
    val rdd = new FrameRDD(schema, rows)

    val structType = rdd.schemaToStructType()
    structType.fields(0).name should be("num")
    structType.fields(0).dataType should be(IntegerType)

    structType.fields(1).name should be("name")
    structType.fields(1).dataType should be(StringType)
  }

  "FrameRDD" should "allow a SchemaRDD in its constructor" in {
    val rows = sparkContext.parallelize((1 to 100).map(i => new TestCase(i, i.toString)))
    val sqlContext = new SQLContext(sparkContext)
    val schemaRDD = sqlContext.createSchemaRDD(rows)
    val schema = new Schema(List(("num", DataTypes.int32), ("name", DataTypes.string)))

    val frameRDD = new FrameRDD(schema, schemaRDD)

    frameRDD.take(1) should be(Array(Array(1, "1")))
  }
}
