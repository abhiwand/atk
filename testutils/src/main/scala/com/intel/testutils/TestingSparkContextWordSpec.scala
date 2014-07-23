package com.intel.testutils

import org.scalatest.{ BeforeAndAfterAll, WordSpec }
import org.apache.spark.SparkContext

trait TestingSparkContextWordSpec extends WordSpec with BeforeAndAfterAll {

  var sparkContext: SparkContext = null

  override def beforeAll = {
    sparkContext = TestingSparkContext.sparkContext
  }

  /**
   * Clean up after the test is done
   */
  override def afterAll = {
    TestingSparkContext.cleanUp()
  }

}
