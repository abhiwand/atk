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
