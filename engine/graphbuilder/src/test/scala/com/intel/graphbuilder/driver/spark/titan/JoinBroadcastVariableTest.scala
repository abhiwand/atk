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

package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.elements.Property
import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

class JoinBroadcastVariableTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  val personIds: List[(Property, AnyRef)] = List(
    (Property("name", "alice"), new java.lang.Long(1)),
    (Property("name", "bob"), new java.lang.Long(2)),
    (Property("name", "cathy"), new java.lang.Long(3)),
    (Property("name", "david"), new java.lang.Long(4)),
    (Property("name", "eva"), new java.lang.Long(5))
  )

  "JoinBroadcastVariable" should "create a single broadcast variable when RDD size is less than 2GB" in {
    val personRdd = sparkContext.parallelize(personIds)

    val broadcastVariable = JoinBroadcastVariable(personRdd)

    broadcastVariable.length() should equal(1)
    broadcastVariable.broadcastMaps(0).value.size should equal(5)
    personIds.map { case (property, id) => broadcastVariable.get(property) should equal(Some(id)) }
    broadcastVariable.get(Property("not defined", new java.lang.Long(1))).isDefined should equal(false)
  }
  "JoinBroadcastVariable" should "create an empty broadcast variable" in {
    val emptyList = sparkContext.parallelize(List.empty[(Property, AnyRef)])

    val broadcastVariable = JoinBroadcastVariable(emptyList)

    broadcastVariable.length() should equal(1)
    broadcastVariable.broadcastMaps(0).value.isEmpty should equal(true)
  }
  "JoinBroadcastVariable" should "throw an IllegalArgumentException if join parameter is null" in {
    intercept[IllegalArgumentException] {
      JoinBroadcastVariable(null)
    }
  }
}
