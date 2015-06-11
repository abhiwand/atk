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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import com.intel.intelanalytics.domain.schema.{ DataTypes, Column }

class EcdfArgsTest extends TestingSparkContextFlatSpec with Matchers {

  // Input data
  val sampleOneList = List(
    Array[Any](0),
    Array[Any](1),
    Array[Any](2),
    Array[Any](3),
    Array[Any](4),
    Array[Any](5),
    Array[Any](6),
    Array[Any](7),
    Array[Any](8),
    Array[Any](9))

  val sampleTwoList = List(
    Array[Any](0),
    Array[Any](0),
    Array[Any](0),
    Array[Any](0),
    Array[Any](4),
    Array[Any](5),
    Array[Any](6),
    Array[Any](7))

  val sampleThreeList = List(
    Array[Any](-2),
    Array[Any](-1),
    Array[Any](0),
    Array[Any](1),
    Array[Any](2))

  "ecdf" should "compute correct ecdf" in {

    val sampleOneRdd = sparkContext.parallelize(sampleOneList, 2)
    val sampleTwoRdd = sparkContext.parallelize(sampleTwoList, 2)
    val sampleThreeRdd = sparkContext.parallelize(sampleThreeList, 2)

    // Get binned results
    val sampleOneECDF = CumulativeDistFunctions.ecdf(sampleOneRdd, Column("a", DataTypes.int32, 0))
    val resultOne = sampleOneECDF.take(10)

    val sampleTwoECDF = CumulativeDistFunctions.ecdf(sampleTwoRdd, Column("a", DataTypes.int32, 0))
    val resultTwo = sampleTwoECDF.take(5)

    val sampleThreeECDF = CumulativeDistFunctions.ecdf(sampleThreeRdd, Column("a", DataTypes.int32, 0))
    val resultThree = sampleThreeECDF.take(5)

    // Validate
    resultOne.apply(0) shouldBe Array[Any](0, 0.1)
    resultOne.apply(1) shouldBe Array[Any](1, 0.2)
    resultOne.apply(2) shouldBe Array[Any](2, 0.3)
    resultOne.apply(3) shouldBe Array[Any](3, 0.4)
    resultOne.apply(4) shouldBe Array[Any](4, 0.5)
    resultOne.apply(5) shouldBe Array[Any](5, 0.6)
    resultOne.apply(6) shouldBe Array[Any](6, 0.7)
    resultOne.apply(7) shouldBe Array[Any](7, 0.8)
    resultOne.apply(8) shouldBe Array[Any](8, 0.9)
    resultOne.apply(9) shouldBe Array[Any](9, 1.0)

    resultTwo.apply(0) shouldBe Array[Any](0, 0.5)
    resultTwo.apply(1) shouldBe Array[Any](4, 0.625)
    resultTwo.apply(2) shouldBe Array[Any](5, 0.75)
    resultTwo.apply(3) shouldBe Array[Any](6, 0.875)
    resultTwo.apply(4) shouldBe Array[Any](7, 1.0)

    resultThree.apply(0) shouldBe Array[Any](-2, 0.2)
    resultThree.apply(1) shouldBe Array[Any](-1, 0.4)
    resultThree.apply(2) shouldBe Array[Any](0, 0.6)
    resultThree.apply(3) shouldBe Array[Any](1, 0.8)
    resultThree.apply(4) shouldBe Array[Any](2, 1.0)
  }

}
