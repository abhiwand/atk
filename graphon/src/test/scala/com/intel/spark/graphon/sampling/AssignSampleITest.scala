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

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.driver.spark.rdd.EnvironmentValidator
import com.intel.graphbuilder.elements.{ Property, GBVertex }
import com.intel.testutils.TestingSparkContextWordSpec
import org.scalatest.Matchers
import com.intel.testutils.{ TestingSparkContextWordSpec, TestingTitan }

class AssignSampleITest extends TestingSparkContextWordSpec with Matchers {
  EnvironmentValidator.skipEnvironmentValidation = true

  val gbIds = Map((1, new Property("gbId", 1)),
    (2, new Property("gbId", 2)),
    (3, new Property("gbId", 3)),
    (4, new Property("gbId", 4)),
    (5, new Property("gbId", 5)),
    (6, new Property("gbId", 6)),
    (7, new Property("gbId", 7)),
    (8, new Property("gbId", 8)))

  val inputVertexList = Seq(GBVertex(gbIds(1), gbIds(1), Set(new Property("number", "1"))),
    GBVertex(gbIds(2), gbIds(2), Set(new Property("number", "2"))),
    GBVertex(gbIds(3), gbIds(3), Set(new Property("number", "3"))),
    GBVertex(gbIds(4), gbIds(4), Set(new Property("number", "4"))),
    GBVertex(gbIds(5), gbIds(5), Set(new Property("number", "5"))),
    GBVertex(gbIds(6), gbIds(6), Set(new Property("number", "6"))),
    GBVertex(gbIds(7), gbIds(7), Set(new Property("number", "7"))),
    GBVertex(gbIds(6), gbIds(6), Set(new Property("number", "8"))),
    GBVertex(gbIds(7), gbIds(7), Set(new Property("number", "9"))),
    GBVertex(gbIds(8), gbIds(8), Set(new Property("number", "10"))))

  //Actual Splits are handled by MLDataSplitter this tests that the plugin creates the proper property

  "AssignSample with titan" should {
    "create the 3 designated labels" in {
      val plugin = new AssignSampleTitanPlugin
      val gbVertices = sparkContext.parallelize(inputVertexList, 2)
      val percentages = List(0.3, 0.3, 0.4)
      val output = "sample"
      val labels = List("1", "2", "3")

      val splitRdd = plugin.splitVertexRDD(gbVertices, percentages, labels, output, 0)

      val results = splitRdd.collect().toList

      val grouped = results.groupBy(gb => gb.getPropertyValueAsString(output))
      val keys = grouped.keys
      keys.size should be(3)
      keys should contain("1")
      keys should contain("2")
      keys should contain("3")
    }

    "create the 5 designated labels" in {
      val plugin = new AssignSampleTitanPlugin
      val gbVertices = sparkContext.parallelize(inputVertexList, 2)
      val percentages = List(0.3, 0.3, 0.2, 0.2)
      val output = "sample"
      val labels = List("1", "2", "3", "4")

      val splitRdd = plugin.splitVertexRDD(gbVertices, percentages, labels, output, 0)

      val results = splitRdd.collect().toList

      val grouped = results.groupBy(gb => gb.getPropertyValueAsString(output))
      val keys = grouped.keys
      keys.size should be(4)
      keys should contain("1")
      keys should contain("2")
      keys should contain("3")
      keys should contain("4")
    }
  }

}
