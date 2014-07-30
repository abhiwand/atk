package com.intel.intelanalytics.engine.spark

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

class EntropyTest extends TestingSparkContextFlatSpec with Matchers {
  val inputList = List(
    Array[Any](0, "a"),
    Array[Any](0, "a"),
    Array[Any](0, "b"),
    Array[Any](1, "b"),
    Array[Any](1, "b"),
    Array[Any](2, "c"))

}
