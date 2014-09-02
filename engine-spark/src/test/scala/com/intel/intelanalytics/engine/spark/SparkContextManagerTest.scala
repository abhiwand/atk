package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, WordSpec }
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.mockito.Mockito._
import com.intel.intelanalytics.engine.spark.context.{ SparkContextManager, SparkContextFactory }
import org.scalatest.mock.MockitoSugar

class SparkContextManagerTest extends WordSpec with MockitoSugar {

  def createMockSparkContextManager(): SparkContextManager = {
    val config = mock[Config]
    val sparkContextFactory = mock[SparkContextFactory]
    val sparkContextManager = new SparkContextManager(config, sparkContextFactory)

    val mockSparkContext_1 = mock[SparkContext]
    val mockSparkContext_2 = mock[SparkContext]
    val mockSparkContext_3 = mock[SparkContext]

    when(config.getString("intel.analytics.engine.spark.home")).thenReturn("")
    when(config.getString("intel.analytics.engine.spark.master")).thenReturn("")
    when(sparkContextFactory.createSparkContext(config, "intel-analytics:user_1:1")).thenReturn(mockSparkContext_1)
    when(sparkContextFactory.createSparkContext(config, "intel-analytics:user_2:2")).thenReturn(mockSparkContext_2)
    when(sparkContextFactory.createSparkContext(config, "intel-analytics:user_3:3")).thenReturn(mockSparkContext_3)
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

      assert(sc_1 != sc_2)
      assert(sc_1 != sc_3)
      assert(sc_2 != sc_3)

      verify(factory, times(1)).createSparkContext(conf, "intel-analytics:user_1:1")
      verify(factory, times(1)).createSparkContext(conf, "intel-analytics:user_2:2")
      verify(factory, times(1)).createSparkContext(conf, "intel-analytics:user_3:3")

      sc_1 = sparkContextManager.getContext("user_1", "1")
      sc_2 = sparkContextManager.getContext("user_2", "2")
      sc_3 = sparkContextManager.getContext("user_3", "3")

      assert(sc_1 != sc_2)
      assert(sc_1 != sc_3)
      assert(sc_2 != sc_3)

      verify(factory, times(2)).createSparkContext(conf, "intel-analytics:user_1:1")
      verify(factory, times(2)).createSparkContext(conf, "intel-analytics:user_2:2")
      verify(factory, times(2)).createSparkContext(conf, "intel-analytics:user_3:3")
    }
  }
}
