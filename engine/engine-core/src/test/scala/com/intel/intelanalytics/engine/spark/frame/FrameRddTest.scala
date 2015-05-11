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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.{ GraphSchema, DataTypes, Schema }
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.sql.catalyst.types.{ StringType, IntegerType }
import org.bson.types.BasicBSONList
import org.bson.{ BSON, BasicBSONObject }
import org.scalatest.Matchers
import org.apache.spark.frame.FrameRdd

import scala.collection.JavaConversions._

class FrameRddTest extends TestingSparkContextWordSpec with Matchers {
  "FrameRdd" should {

    "create an appropriate StructType from frames Schema" in {
      val schema = Schema.fromTuples(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val structType = FrameRdd.schemaToStructType(schema.columnTuples)
      structType.fields(0).name should be("num")
      structType.fields(0).dataType should be(IntegerType)

      structType.fields(1).name should be("name")
      structType.fields(1).dataType should be(StringType)
    }

    "allow a Row RDD in the construtor" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
      val schema = Schema.fromTuples(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val rdd = FrameRdd.toFrameRdd(schema, rows)
      rdd.frameSchema should be(schema)
      rdd.first should equal(rows.first)
    }

    "be convertible into a LegacyFrameRdd" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
      val schema = Schema.fromTuples(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val rdd = FrameRdd.toFrameRdd(schema, rows)
      val legacy = rdd.toLegacyFrameRdd
      legacy.getClass should be(classOf[LegacyFrameRdd])
      legacy.schema should equal(schema)
      legacy.first should equal(rdd.first)
    }

    "create unique ids in a new column" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i.toLong, i.toString))).repartition(7)
      val schema = Schema.fromTuples(List(("num", DataTypes.int64), ("name", DataTypes.string)))
      val rdd = FrameRdd.toFrameRdd(schema, rows)

      val rddWithUniqueIds = rdd.assignUniqueIds(GraphSchema.vidProperty)
      rddWithUniqueIds.frameSchema.columnTuples.size should be(3)
      val ids = rddWithUniqueIds.map(x => x(2)).collect

      val uniqueIds = ids.distinct
      ids.size should equal(uniqueIds.size)
      ids(0) should be(0)
    }

    "create unique ids in an existing column" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i.toLong, i.toString))).repartition(5)
      val schema = Schema.fromTuples(List(("num", DataTypes.int64), ("name", DataTypes.string)))
      val rdd = FrameRdd.toFrameRdd(schema, rows)

      val rddWithUniqueIds = rdd.assignUniqueIds("num")
      rddWithUniqueIds.frameSchema.columnTuples.size should be(2)
      val values = rddWithUniqueIds.collect()
      val ids = values.map(x => x(0))

      val uniqueIds = ids.distinct
      ids.size should equal(uniqueIds.size)
      ids(0) should be(0)
    }

    "create unique ids starting at a specified value" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i.toLong, i.toString))).repartition(3)
      val schema = Schema.fromTuples(List(("num", DataTypes.int64), ("name", DataTypes.string)))
      val rdd = FrameRdd.toFrameRdd(schema, rows)
      val startVal = 121

      val rddWithUniqueIds = rdd.assignUniqueIds("num", startVal)
      val values = rddWithUniqueIds.collect()
      val ids = values.map(x => x(0))

      val uniqueIds = ids.distinct
      ids.size should equal(uniqueIds.size)
      ids(0) should be(startVal)

    }
  }
}
