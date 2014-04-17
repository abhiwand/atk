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

package com.intel.graphbuilder.elements

import org.specs2.mutable.Specification

class EdgeSpec extends Specification {

  val tailId = new Property("gbId", 10001)
  val headId = new Property("gbId", 10002)
  val label = "myLabel"
  val edge = new Edge(tailId, headId, label, List(new Property("key", "value")))

  "Edge" should {

    "be reverse-able" in {
      // invoke method under test
      val reversedEdge = edge.reverse()

      // should be opposite
      edge.headVertexGbId mustEqual reversedEdge.tailVertexGbId
      edge.tailVertexGbId mustEqual reversedEdge.headVertexGbId

      // should be same same
      edge.label mustEqual reversedEdge.label
      edge.properties mustEqual reversedEdge.properties
    }

    "have a unique id made up of the tailId, headId, and label" in {
      edge.id mustEqual(tailId, headId, label)
    }

    "be mergeable" in {
      val edge2 = new Edge(tailId, headId, label, List(new Property("otherKey", "otherValue")))

      // invoke method under test
      val merged = edge.merge(edge2)

      merged.properties.size mustEqual 2
      merged.properties(0).key mustEqual "key"
      merged.properties(1).key mustEqual "otherKey"
    }

    "not allow merging of edges with different ids" in {
      val diffId = new Property("gbId", 9999)
      val edge2 = new Edge(tailId, diffId, label, List(new Property("otherKey", "otherValue")))

      edge.merge(edge2) must throwA[IllegalArgumentException]
    }
  }
}
