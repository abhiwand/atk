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

class VertexSpec extends Specification {

  val gbId = new Property("gbId", 10001)
  val vertex = Vertex(gbId, List(new Property("key", "value")))

  "Vertex" should {
    "have a unique id that is the gbId" in {
      vertex.id mustEqual gbId
    }

    "be mergeable with another vertex" in {
      val vertex2 = new Vertex(gbId, List(new Property("anotherKey", "anotherValue")))

      // invoke method under test
      val merged = vertex.merge(vertex2)

      merged.gbId mustEqual gbId
      merged.properties.size mustEqual 2

      merged.properties(0).key mustEqual "key"
      merged.properties(0).value mustEqual "value"

      merged.properties(1).key mustEqual "anotherKey"
      merged.properties(1).value mustEqual "anotherValue"
    }

    "not allow null gbIds" in {
      new Vertex(null, Nil) must throwA[IllegalArgumentException]
    }

    "not allow merging of vertices with different ids" in {
      val diffId = new Property("gbId", 10002)
      val vertex2 = new Vertex(diffId, List(new Property("anotherKey", "anotherValue")))

      // invoke method under test
      vertex.merge(vertex2) must throwA[IllegalArgumentException]
    }
  }

}
