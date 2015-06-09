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

class SparkJoinITest extends TestingSparkContextFlatSpec with Matchers {
  // Test data has duplicate keys, matching and non-matching keys
  val idCountryCodes: List[(Any, sql.Row)] = List(
    (1.asInstanceOf[Any], new GenericRow(Array[Any](1, 354))),
    (2.asInstanceOf[Any], new GenericRow(Array[Any](2, 91))),
    (2.asInstanceOf[Any], new GenericRow(Array[Any](2, 100))),
    (3.asInstanceOf[Any], new GenericRow(Array[Any](3, 47))),
    (4.asInstanceOf[Any], new GenericRow(Array[Any](4, 968))),
    (5.asInstanceOf[Any], new GenericRow(Array[Any](5, 50))))

  val idCountryNames: List[(Any, sql.Row)] = List(
    (1.asInstanceOf[Any], new GenericRow(Array[Any](1, "Iceland"))),
    (1.asInstanceOf[Any], new GenericRow(Array[Any](1, "Ice-land"))),
    (2.asInstanceOf[Any], new GenericRow(Array[Any](2, "India"))),
    (3.asInstanceOf[Any], new GenericRow(Array[Any](3, "Norway"))),
    (4.asInstanceOf[Any], new GenericRow(Array[Any](4, "Oman"))),
    (6.asInstanceOf[Any], new GenericRow(Array[Any](6, "Germany")))
  )

  "joinRDDs" should "join two RDD with inner join" in {

    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, 2), RddJoinParam(countryNames, 2), "inner").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman"))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "joinRDDs" should "join two RDD with inner join using broadcast variable" in {
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val broadcastJoinThreshold = 1000
    val leftJoinParam = RddJoinParam(countryCode, 2, Some(150))
    val rightJoinParam = RddJoinParam(countryNames, 2, Some(10000))

    val results = JoinRddFunctions.joinRDDs(leftJoinParam, rightJoinParam, "inner").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman"))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "joinRDDs" should "join two RDD with left join" in {
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, 2), RddJoinParam(countryNames, 2), "left").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](5, 50, null, null))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "joinRDDs" should "join two RDD with left join using broadcast variable" in {
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val broadcastJoinThreshold = 1000L + Int.MaxValue
    val leftJoinParam = RddJoinParam(countryCode, 2, Some(1500L))
    val rightJoinParam = RddJoinParam(countryNames, 2, Some(100L + Int.MaxValue))

    // Test join wrapper function
    val results = JoinRddFunctions.joinRDDs(leftJoinParam, rightJoinParam, "left").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](5, 50, null, null))
    )

    results should contain theSameElementsAs (expectedResults)
  }
  "joinRDDs" should "join two RDD with right join" in {
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, 2), RddJoinParam(countryNames, 2), "right").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "joinRDDs" should "join two RDD with right join using broadcast variable" in {
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val broadcastJoinThreshold = 1000
    val leftJoinParam = RddJoinParam(countryCode, 2, Some(800))
    val rightJoinParam = RddJoinParam(countryNames, 2, Some(4000))

    val results = JoinRddFunctions.joinRDDs(leftJoinParam, rightJoinParam, "right", broadcastJoinThreshold).collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "joinRDDs" should "join two RDD with outer join" in {
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, 2), RddJoinParam(countryNames, 2), "outer").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](5, 50, null, null)),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "outer join with empty left RDD" should "preserve the result from the right RDD" in {
    val emptyIdCountryCodes = List.empty[(Any, sql.Row)]
    val countryCode = sparkContext.parallelize(emptyIdCountryCodes)
    val countryNames = sparkContext.parallelize(idCountryNames)

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, 2), RddJoinParam(countryNames, 2), "outer").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](null, null, 1, "Iceland")),
      new GenericRow(Array[Any](null, null, 1, "Ice-land")),
      new GenericRow(Array[Any](null, null, 2, "India")),
      new GenericRow(Array[Any](null, null, 3, "Norway")),
      new GenericRow(Array[Any](null, null, 4, "Oman")),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "outer join with empty right RDD" should "preserve the result from the left RDD" in {
    val emptyIdCountryNames = List.empty[(Any, sql.Row)]
    val countryCode = sparkContext.parallelize(idCountryCodes)
    val countryNames = sparkContext.parallelize(emptyIdCountryNames)

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, 2), RddJoinParam(countryNames, 2), "outer").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, null, null)),
      new GenericRow(Array[Any](2, 91, null, null)),
      new GenericRow(Array[Any](2, 100, null, null)),
      new GenericRow(Array[Any](3, 47, null, null)),
      new GenericRow(Array[Any](4, 968, null, null)),
      new GenericRow(Array[Any](5, 50, null, null))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "outer join large RDD" should "generate RDD contains all element from both RDD" in {
    val oneToOneHundredThousand: List[(Any, sql.Row)] = (1 to 100000).map(i => {
      (i.asInstanceOf[Any], new GenericRow(Array[Any](i)))
    }).toList

    val fiftyThousandToOneFiftyThousands: List[(Any, sql.Row)] = (50001 to 150000).map(i => {
      (i.asInstanceOf[Any], new GenericRow(Array[Any](i)))
    }).toList

    val rddOneToMillon = sparkContext.parallelize(oneToOneHundredThousand)
    val rddFiveHundredThousandsToOneFiftyThousands = sparkContext.parallelize(fiftyThousandToOneFiftyThousands)

    val rddFullOuterJoin = JoinRddFunctions.joinRDDs(RddJoinParam(rddOneToMillon, 1), RddJoinParam(rddFiveHundredThousandsToOneFiftyThousands, 1), "outer")
    rddFullOuterJoin.count shouldBe 150000
  }

}
