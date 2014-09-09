package com.intel.testutils

import org.apache.spark.SparkContext
import org.scalatest.{ FunSuite, BeforeAndAfterAll }

trait TestingSparkContextFunSuite extends FunSuite with BeforeAndAfterAll {

  var sparkContext: SparkContext = null

  override def beforeAll() = {
    sparkContext = TestingSparkContext.sparkContext
  }

  /**
   * Clean up after the test is done
   */
  override def afterAll() = {
    sparkContext = null
    TestingSparkContext.cleanUp()
  }

}

