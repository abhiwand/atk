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

import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import org.apache.spark.frame.FrameRdd

/**
 * Sample Class used for a test case below
 * @param num  a number
 * @param name a string representation of that number
 */
case class TestCase(num: Int, name: String)

/**
 * Unit Tests for the FrameRdd class
 */
class LegacyFrameRddTest extends TestingSparkContextWordSpec with Matchers {
  "LegacyFrameRdd" should {

    "be convertible into a FrameRdd" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => Array(i, i.toString)))
      val schema = Schema.fromTuples(List(("num", DataTypes.int32), ("name", DataTypes.string)))
      val rdd = new LegacyFrameRdd(schema, rows)
      val frameRdd = rdd.toFrameRdd()

      frameRdd.getClass should be(classOf[FrameRdd])
      frameRdd.frameSchema should be(schema)
      frameRdd.first should equal(rdd.first)
    }

    // ignoring because of OutOfMemory errors, these weren't showing up in engine until most of shared was merged in
    "allow a SchemaRDD in its constructor" in {
      val rows = sparkContext.parallelize((1 to 100).map(i => new TestCase(i, i.toString)))
      val sqlContext = new SQLContext(sparkContext)
      val dataframe = sqlContext.createDataFrame(rows)
      val schema = Schema.fromTuples(List(("num", DataTypes.int32), ("name", DataTypes.string)))

      val legacyFrameRdd = new LegacyFrameRdd(schema, dataframe)

      legacyFrameRdd.take(1) should be(Array(Array(1, "1")))
    }
  }
}
