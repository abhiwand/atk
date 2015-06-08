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

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.elements.{ Property, GBVertex }
import com.intel.testutils.TestingSparkContextWordSpec
import org.scalatest.Matchers
import com.intel.testutils.{ TestingSparkContextWordSpec, TestingTitan }

class AssignSampleITest extends TestingSparkContextWordSpec with Matchers {

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
