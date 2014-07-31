package com.intel.intelanalytics.engine.spark

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import com.intel.intelanalytics.engine.spark.context.{ SparkContextManager, SparkContextFactory }

class SparkContextManagerSpec extends Specification with Mockito {

  def createMockSparkContextManager(): SparkContextManager = {
    val mockConfig = mock[Config]
    val mockSparkContextFactory = mock[SparkContextFactory]
    val sparkContextManager = new SparkContextManager(mockConfig, mockSparkContextFactory)

    val mockSparkContext_1 = mock[SparkContext]
    val mockSparkContext_2 = mock[SparkContext]
    val mockSparkContext_3 = mock[SparkContext]

    mockConfig.getString("intel.analytics.engine.spark.home") returns ""
    mockConfig.getString("intel.analytics.engine.spark.master") returns ""
    mockSparkContextFactory.createSparkContext(mockConfig, "intel-analytics:user_1:1") returns mockSparkContext_1
    mockSparkContextFactory.createSparkContext(mockConfig, "intel-analytics:user_2:2") returns mockSparkContext_2
    mockSparkContextFactory.createSparkContext(mockConfig, "intel-analytics:user_3:3") returns mockSparkContext_3
    sparkContextManager
  }

  "SparkContextManager" should {


    "create a new context everytime" in {

      val sparkContextManager = createMockSparkContextManager()
      val factory = sparkContextManager.contextManagementStrategy.sparkContextFactory
      val conf = sparkContextManager.contextManagementStrategy.configuration

      var sc_1: SparkContext = sparkContextManager.getContext("user_1", "1")
      var sc_2: SparkContext = sparkContextManager.getContext("user_2", "2")
      var sc_3: SparkContext = sparkContextManager.getContext("user_3", "3")

      sc_1 shouldNotEqual sc_2
      sc_1 shouldNotEqual sc_3
      sc_2 shouldNotEqual sc_3

      there was one(factory).createSparkContext(conf, "intel-analytics:user_1:1")
      there was one(factory).createSparkContext(conf, "intel-analytics:user_2:2")
      there was one(factory).createSparkContext(conf, "intel-analytics:user_3:3")

      sc_1 = sparkContextManager.getContext("user_1", "1")
      sc_2 = sparkContextManager.getContext("user_2", "2")
      sc_3 = sparkContextManager.getContext("user_3", "3")

      sc_1 shouldNotEqual sc_2
      sc_1 shouldNotEqual sc_3
      sc_2 shouldNotEqual sc_3

      there was two(factory).createSparkContext(conf, "intel-analytics:user_1:1")
      there was two(factory).createSparkContext(conf, "intel-analytics:user_2:2")
      there was two(factory).createSparkContext(conf, "intel-analytics:user_3:3")
    }
  }
}
