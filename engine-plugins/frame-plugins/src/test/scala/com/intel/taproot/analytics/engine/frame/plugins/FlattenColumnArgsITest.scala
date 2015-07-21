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

package com.intel.taproot.analytics.engine.frame.plugins

import com.intel.taproot.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql.Row
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }

class FlattenColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByStringColumnIndex" should "create separate rows when flattening entries" in {
    val carOwnerShips = List(Row("Bob", "Mustang,Camry"), Row("Josh", "Neon,CLK"), Row("Alice", "PT Cruiser,Avalon,F-150"), Row("Tim", "Beatle"), Row("Becky", ""))
    val rdd = sparkContext.parallelize(carOwnerShips)
    val flattened = FlattenColumnFunctions.flattenRddByStringColumnIndex(1, ",")(rdd)
    val result = flattened.take(9)
    result.apply(0) shouldBe Row("Bob", "Mustang")
    result.apply(1) shouldBe Row("Bob", "Camry")
    result.apply(2) shouldBe Row("Josh", "Neon")
    result.apply(3) shouldBe Row("Josh", "CLK")
    result.apply(4) shouldBe Row("Alice", "PT Cruiser")
    result.apply(5) shouldBe Row("Alice", "Avalon")
    result.apply(6) shouldBe Row("Alice", "F-150")
    result.apply(7) shouldBe Row("Tim", "Beatle")
    result.apply(8) shouldBe Row("Becky", "")

  }
}

class FlattenVectorColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByVectorColumnIndex" should "create separate rows when flattening entries" in {
    val vectorShips = List(Row("counting", Vector[Double](1.2, 3.4, 5.6)),
      Row("pi", Vector[Double](3.14, 6.28, 9.42)),
      Row("neg", Vector[Double](-1.0, -5.5, 8)))
    val rdd = sparkContext.parallelize(vectorShips)
    val flattened = FlattenColumnFunctions.flattenRddByVectorColumnIndex(1, 3)(rdd)
    val result = flattened.take(9)
    result.apply(0) shouldBe Row("counting", 1.2)
    result.apply(1) shouldBe Row("counting", 3.4)
    result.apply(2) shouldBe Row("counting", 5.6)
    result.apply(3) shouldBe Row("pi", 3.14)
    result.apply(4) shouldBe Row("pi", 6.28)
    result.apply(5) shouldBe Row("pi", 9.42)
    result.apply(6) shouldBe Row("neg", -1.0)
    result.apply(7) shouldBe Row("neg", -5.5)
    result.apply(8) shouldBe Row("neg", 8)
  }
}
