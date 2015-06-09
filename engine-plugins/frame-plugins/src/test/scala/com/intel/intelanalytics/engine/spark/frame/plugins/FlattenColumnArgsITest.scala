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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }

class FlattenColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByStringColumnIndex" should "create separate rows when flattening entries" in {
    val carOwnerShips = List(Array[Any]("Bob", "Mustang,Camry"), Array[Any]("Josh", "Neon,CLK"), Array[Any]("Alice", "PT Cruiser,Avalon,F-150"), Array[Any]("Tim", "Beatle"), Array[Any]("Becky", ""))
    val rdd = sparkContext.parallelize(carOwnerShips)
    val flattened = FlattenColumnFunctions.flattenRddByStringColumnIndex(1, ",")(rdd)
    val result = flattened.take(9)
    result.apply(0) shouldBe Array[Any]("Bob", "Mustang")
    result.apply(1) shouldBe Array[Any]("Bob", "Camry")
    result.apply(2) shouldBe Array[Any]("Josh", "Neon")
    result.apply(3) shouldBe Array[Any]("Josh", "CLK")
    result.apply(4) shouldBe Array[Any]("Alice", "PT Cruiser")
    result.apply(5) shouldBe Array[Any]("Alice", "Avalon")
    result.apply(6) shouldBe Array[Any]("Alice", "F-150")
    result.apply(7) shouldBe Array[Any]("Tim", "Beatle")
    result.apply(8) shouldBe Array[Any]("Becky", "")

  }
}

class FlattenVectorColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByVectorColumnIndex" should "create separate rows when flattening entries" in {
    val vectorShips = List(Array[Any]("counting", Vector[Double](1.2, 3.4, 5.6)),
      Array[Any]("pi", Vector[Double](3.14, 6.28, 9.42)),
      Array[Any]("neg", Vector[Double](-1.0, -5.5, 8)))
    val rdd = sparkContext.parallelize(vectorShips)
    val flattened = FlattenColumnFunctions.flattenRddByVectorColumnIndex(1, 3)(rdd)
    val result = flattened.take(9)
    result.apply(0) shouldBe Array[Any]("counting", 1.2)
    result.apply(1) shouldBe Array[Any]("counting", 3.4)
    result.apply(2) shouldBe Array[Any]("counting", 5.6)
    result.apply(3) shouldBe Array[Any]("pi", 3.14)
    result.apply(4) shouldBe Array[Any]("pi", 6.28)
    result.apply(5) shouldBe Array[Any]("pi", 9.42)
    result.apply(6) shouldBe Array[Any]("neg", -1.0)
    result.apply(7) shouldBe Array[Any]("neg", -5.5)
    result.apply(8) shouldBe Array[Any]("neg", 8)
  }
}
