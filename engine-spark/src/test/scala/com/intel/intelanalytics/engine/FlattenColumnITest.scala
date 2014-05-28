package com.intel.intelanalytics.engine.spark

import org.scalatest.{ BeforeAndAfterEach, Matchers, FlatSpec }
import com.intel.intelanalytics.engine.TestingSparkContext

class FlattenColumnITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContext {
  "flattenRddByColumnIndex" should "create separate rows when flattening entries" in {
    val carOwnerShips = List(Array[Any]("Bob", "Mustang,Camry"), Array[Any]("Josh", "Neon,CLK"), Array[Any]("Alice", "PT Cruiser,Avalon,F-150"), Array[Any]("Tim", "Beatle"))
    val rdd = sc.parallelize(carOwnerShips)
    val flattened = SparkOps.flattenRddByColumnIndex(1, ",", rdd)
    val result = flattened.take(8)
    result.apply(0) shouldBe Array[Any]("Bob", "Mustang")
    result.apply(1) shouldBe Array[Any]("Bob", "Camry")
    result.apply(2) shouldBe Array[Any]("Josh", "Neon")
    result.apply(3) shouldBe Array[Any]("Josh", "CLK")
    result.apply(4) shouldBe Array[Any]("Alice", "PT Cruiser")
    result.apply(5) shouldBe Array[Any]("Alice", "Avalon")
    result.apply(6) shouldBe Array[Any]("Alice", "F-150")
    result.apply(7) shouldBe Array[Any]("Tim", "Beatle")

  }

}
