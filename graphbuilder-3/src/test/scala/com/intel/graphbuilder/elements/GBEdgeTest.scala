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

package com.intel.graphbuilder.elements

import org.scalatest.{ Matchers, WordSpec }

class GBEdgeTest extends WordSpec with Matchers {

  val tailId = new Property("gbId", 10001)
  val headId = new Property("gbId", 10002)
  val label = "myLabel"
  val edge = new GBEdge(None, tailId, headId, label, Set(new Property("key", "value")))

  "Edge" should {

    "be reverse-able" in {
      // invoke method under test
      val reversedEdge = edge.reverse()

      // should be opposite
      edge.headVertexGbId shouldBe reversedEdge.tailVertexGbId
      edge.tailVertexGbId shouldBe reversedEdge.headVertexGbId

      // should be same same
      edge.label shouldBe reversedEdge.label
      edge.properties shouldBe reversedEdge.properties
    }

    "have a unique id made up of the tailId, headId, and label" in {
      edge.id shouldBe (tailId, headId, label)
    }

    "be mergeable" in {
      val edge2 = new GBEdge(None, tailId, headId, label, Set(new Property("otherKey", "otherValue")))

      // invoke method under test
      val merged = edge.merge(edge2)

      merged.properties shouldBe Set(Property("key", "value"), Property("otherKey", "otherValue"))

    }

    "not allow merging of edges with different ids" in {
      val diffId = new Property("gbId", 9999)
      val edge2 = new GBEdge(None, tailId, diffId, label, Set(new Property("otherKey", "otherValue")))

      intercept[IllegalArgumentException] {
        edge.merge(edge2)
      }
    }
  }
}
