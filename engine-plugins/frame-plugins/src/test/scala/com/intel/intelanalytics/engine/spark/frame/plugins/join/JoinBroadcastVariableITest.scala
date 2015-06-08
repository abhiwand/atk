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
