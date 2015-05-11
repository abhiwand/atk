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

package org.apache.spark.api.python

import org.scalatest.WordSpec

class EnginePythonAccumulatorParamTest extends WordSpec {

  val accum = new EnginePythonAccumulatorParam()

  "EnginePythonAccumulatorParam" should {
    "have a zero value" in {
      assert(accum.zero(new java.util.ArrayList()).size() == 0)
    }

    "have a zero value when given a value" in {
      val list = new java.util.ArrayList[Array[Byte]]()
      list.add(Array[Byte](0))
      assert(accum.zero(list).size() == 0)
    }

    "should be able to add in place" in {
      val accum2 = new EnginePythonAccumulatorParam()

      val list1 = new java.util.ArrayList[Array[Byte]]()
      list1.add(Array[Byte](0))

      val list2 = new java.util.ArrayList[Array[Byte]]()
      list2.add(Array[Byte](0))

      assert(accum2.addInPlace(list1, list2).size() == 2)
    }
  }
}
