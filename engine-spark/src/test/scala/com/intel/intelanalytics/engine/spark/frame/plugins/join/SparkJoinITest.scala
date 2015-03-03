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

    val results = JoinRDDFunctions.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "inner").collect()

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
    val leftJoinParam = RDDJoinParam(countryCode, 2, Some(150))
    val rightJoinParam = RDDJoinParam(countryNames, 2, Some(10000))

    val results = JoinRDDFunctions.joinRDDs(leftJoinParam, rightJoinParam, "inner").collect()

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

    val results = JoinRDDFunctions.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "left").collect()

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
    val leftJoinParam = RDDJoinParam(countryCode, 2, Some(1500L))
    val rightJoinParam = RDDJoinParam(countryNames, 2, Some(100L + Int.MaxValue))

    // Test join wrapper function
    val results = JoinRDDFunctions.joinRDDs(leftJoinParam, rightJoinParam, "left").collect()

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

    val results = JoinRDDFunctions.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "right").collect()

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
    val leftJoinParam = RDDJoinParam(countryCode, 2, Some(800))
    val rightJoinParam = RDDJoinParam(countryNames, 2, Some(4000))

    val results = JoinRDDFunctions.joinRDDs(leftJoinParam, rightJoinParam, "right", broadcastJoinThreshold).collect()

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

    val results = JoinRDDFunctions.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "outer").collect()

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

    val results = JoinRDDFunctions.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "outer").collect()

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

    val results = JoinRDDFunctions.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "outer").collect()

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

    val rddFullOuterJoin = JoinRDDFunctions.joinRDDs(RDDJoinParam(rddOneToMillon, 1), RDDJoinParam(rddFiveHundredThousandsToOneFiftyThousands, 1), "outer")
    rddFullOuterJoin.count shouldBe 150000
  }

}
