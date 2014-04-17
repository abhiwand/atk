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

class PropertySpec extends Specification {

  "Property" should {

    "merge 2 properties with the same key to 1" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyA", "valueA")

      val result = Property.merge(List(p1), List(p2))

      result.size mustEqual 1
      result(0).key mustEqual "keyA"
      result(0).value mustEqual "valueA"
    }

    "merge 2 properties with different keys to 2" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyB", "valueB")

      val result = Property.merge(List(p1), List(p2))

      result.size mustEqual 2
      result(0).key mustEqual "keyA"
      result(0).value mustEqual "valueA"
      result(1).key mustEqual "keyB"
      result(1).value mustEqual "valueB"
    }

    "merge 7 properties with mixture of same/different keys to 5" in {
      val p1 = new Property("keyA", "valueA")
      val p2 = new Property("keyB", "valueB")
      val p3 = new Property("keyC", "valueC")
      val p4 = new Property("keyB", "valueB2")
      val p5 = new Property("keyD", "valueD")
      val p6 = new Property("keyA", "valueA2")
      val p7 = new Property("keyE", "valueE")

      val result = Property.merge(List(p1, p2, p3, p4), List(p5, p6, p7))

      result.size mustEqual 5
    }

    "provide convenience constructor" in {
      val p = new Property(1, 2)
      p.key mustEqual "1" // key 1 is converted to a String
      p.value mustEqual 2
    }
  }
}
