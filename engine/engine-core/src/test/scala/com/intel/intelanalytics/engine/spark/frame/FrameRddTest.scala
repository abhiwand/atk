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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.{ GraphSchema, DataTypes, Schema }
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.sql.types.{ StringType, IntegerType }
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
      rdd.first.toSeq.toArray should equal(rows.first)
    }

    "be convertible into a LegacyFrameRdd" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
      val schema = Schema.fromTuples(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val rdd = FrameRdd.toFrameRdd(schema, rows)
      val legacy = rdd.toLegacyFrameRdd
      legacy.getClass should be(classOf[LegacyFrameRdd])
      legacy.schema should equal(schema)
      legacy.first should equal(rdd.first.toSeq.toArray)
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
