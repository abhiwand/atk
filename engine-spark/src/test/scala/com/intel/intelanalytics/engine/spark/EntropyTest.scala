package com.intel.intelanalytics.engine.spark

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

class EntropyTest extends TestingSparkContextFlatSpec with Matchers {
  val inputList = List(
    Array[Any](-1, "a", 0),
    Array[Any](0, "a", 0),
    Array[Any](0, "b", 0),
    Array[Any](1, "b", 0),
    Array[Any](1, "b", 0),
    Array[Any](2, "c", 0))

  val emptyList = List.empty[Array[Any]]

  val epsilon = 0.000001
  "entropy" should "compute the correct entropy" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val entropy1 = SparkOps.entropy(frameRdd, 0)
    val entropy2 = SparkOps.entropy(frameRdd, 1)
    val entropy3 = SparkOps.entropy(frameRdd, 2)

    entropy1 should equal(1.329661 +- epsilon)
    entropy2 should equal(1.011404 +- epsilon)
    entropy3 should equal(0)
  }
  "entropy" should "should throw a runtime exception if the frame is empty" in {
    intercept[java.lang.RuntimeException] {
      val frameRdd = sparkContext.parallelize(emptyList, 2)
      SparkOps.entropy(frameRdd, 0)
    }
  }
}
