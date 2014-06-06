package com.intel.intelanalytics.engine.spark

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import com.intel.intelanalytics.engine.spark.context.{SparkContextManager, SparkContextFactory}

class SparkContextManagerSpec extends Specification with Mockito {

  def createMockSparkContextManager(): SparkContextManager = {
    val mockConfig = mock[Config]
    val mockSparkContextFactory = mock[SparkContextFactory]
    val sparkContextManager = new SparkContextManager(mockConfig, mockSparkContextFactory)

    val mockSparkContext_1 = mock[SparkContext]
    val mockSparkContext_2 = mock[SparkContext]
    val mockSparkContext_3 = mock[SparkContext]

    mockConfig.getString("intel.analytics.spark.home") returns ""
    mockConfig.getString("intel.analytics.spark.master") returns ""
    mockSparkContextFactory.createSparkContext(mockConfig, "intel-analytics:user_1") returns mockSparkContext_1
    mockSparkContextFactory.createSparkContext(mockConfig, "intel-analytics:user_2") returns mockSparkContext_2
    mockSparkContextFactory.createSparkContext(mockConfig, "intel-analytics:user_3") returns mockSparkContext_3
    sparkContextManager
  }

  "SparkContextManager" should {

    val sparkContextManager = createMockSparkContextManager()

    "create a new context for a user if it doesn't exist" in {
      val sc_1: SparkContext = sparkContextManager.getContext("user_1").sparkContext
      val sc_2: SparkContext = sparkContextManager.getContext("user_2").sparkContext
      val sc_3: SparkContext = sparkContextManager.getContext("user_3").sparkContext

      sc_1 shouldNotEqual sc_2
      sc_1 shouldNotEqual sc_3
      sc_2 shouldNotEqual sc_3
      //      sparkContextManager.getAllContexts().size shouldEqual 2 //this test somehow causes JVM to crash with permgen
      // space problems, mocking issues probably
    }

    "return a spark context for a user if it already exists" in {
      val sc_1: SparkContext = sparkContextManager.getContext("user_1").sparkContext
      val sc_2: SparkContext = sparkContextManager.getContext("user_2").sparkContext
      val sc_3: SparkContext = sparkContextManager.getContext("user_3").sparkContext
      val sc_4: SparkContext = sparkContextManager.getContext("user_1").sparkContext //should be same as sc_1
      val sc_5: SparkContext = sparkContextManager.getContext("user_2").sparkContext //should be same as sc_2
      val sc_6: SparkContext = sparkContextManager.getContext("user_3").sparkContext //should be same as sc_3
      sc_1 shouldEqual sc_4
      sc_2 shouldEqual sc_5
      sc_3 shouldEqual sc_6
    }
  }
}
