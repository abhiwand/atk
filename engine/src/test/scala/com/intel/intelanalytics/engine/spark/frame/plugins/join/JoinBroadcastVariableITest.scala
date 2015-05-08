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

package com.intel.intelanalytics.engine.spark.frame.plugins.join

import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers

class JoinBroadcastVariableITest extends TestingSparkContextFlatSpec with Matchers {
  val idCountryNames: List[(Any, sql.Row)] = List(
    (1.asInstanceOf[Any], new GenericRow(Array[Any](1, "Iceland"))),
    (1.asInstanceOf[Any], new GenericRow(Array[Any](1, "Ice-land"))),
    (2.asInstanceOf[Any], new GenericRow(Array[Any](2, "India"))),
    (3.asInstanceOf[Any], new GenericRow(Array[Any](3, "Norway"))),
    (4.asInstanceOf[Any], new GenericRow(Array[Any](4, "Oman"))),
    (6.asInstanceOf[Any], new GenericRow(Array[Any](6, "Germany")))
  )

  "JoinBroadcastVariable" should "create a single broadcast variable when RDD size is less than 2GB" in {
    val countryNames = sparkContext.parallelize(idCountryNames)

    val joinParam = RddJoinParam(countryNames, 2, Some(150))

    val broadcastVariable = JoinBroadcastVariable(joinParam)

    broadcastVariable.length() should equal(1)
    broadcastVariable.broadcastMultiMaps(0).value.size should equal(5)
    broadcastVariable.get(1).get should contain theSameElementsAs Set(idCountryNames(0)._2, idCountryNames(1)._2)
    broadcastVariable.get(2).get should contain theSameElementsAs Set(idCountryNames(2)._2)
    broadcastVariable.get(3).get should contain theSameElementsAs Set(idCountryNames(3)._2)
    broadcastVariable.get(4).get should contain theSameElementsAs Set(idCountryNames(4)._2)
    broadcastVariable.get(6).get should contain theSameElementsAs Set(idCountryNames(5)._2)
    broadcastVariable.get(8).isDefined should equal(false)

  }
  "JoinBroadcastVariable" should "create a two broadcast variables when RDD size is equals 3GB" in {
    val countryNames = sparkContext.parallelize(idCountryNames)

    val joinParam = RddJoinParam(countryNames, 2, Some(3L * 1024 * 1024 * 1024))

    val broadcastVariable = JoinBroadcastVariable(joinParam)

    broadcastVariable.length() should equal(2)
    broadcastVariable.broadcastMultiMaps(0).value.size + broadcastVariable.broadcastMultiMaps(1).value.size should equal(5)
    broadcastVariable.get(1).get should contain theSameElementsAs Set(idCountryNames(0)._2, idCountryNames(1)._2)
    broadcastVariable.get(2).get should contain theSameElementsAs Set(idCountryNames(2)._2)
    broadcastVariable.get(3).get should contain theSameElementsAs Set(idCountryNames(3)._2)
    broadcastVariable.get(4).get should contain theSameElementsAs Set(idCountryNames(4)._2)
    broadcastVariable.get(6).get should contain theSameElementsAs Set(idCountryNames(5)._2)
    broadcastVariable.get(8).isDefined should equal(false)
  }
  "JoinBroadcastVariable" should "create an empty broadcast variable" in {
    val countryNames = sparkContext.parallelize(List.empty[(Any, sql.Row)])

    val joinParam = RddJoinParam(countryNames, 2, Some(3L * 1024 * 1024 * 1024))

    val broadcastVariable = JoinBroadcastVariable(joinParam)

    broadcastVariable.length() should equal(1)
    broadcastVariable.broadcastMultiMaps(0).value.isEmpty should equal(true)
  }
  "JoinBroadcastVariable" should "throw an IllegalArgumentException if join parameter is null" in {
    intercept[IllegalArgumentException] {
      JoinBroadcastVariable(null)
    }
  }

}
