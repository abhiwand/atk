package com.intel.graphbuilder.testutils

import java.util.Date
import org.apache.commons.lang3.RandomUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.After

/**
 * This trait case be mixed into Specifications to create a SparkContext for testing.
 *
 * IMPORTANT! this adds a couple seconds to your unit test!
 */
class TestingSparkContext extends After {

  //LogUtils.silenceSpark()

  lazy val conf = new SparkConf()
    .setMaster("local")
    .setAppName("test " + new Date())
    .set("spark.ui.port", availablePort().toString)

  lazy val sc = new SparkContext(conf)

  override def after: Any = {
    cleanupSpark()
  }

  def cleanupSpark(): Unit = {
    try {
      if (sc != null) {
        sc.stop()
      }
    }
    finally {
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
      //System.clearProperty("spark.hostPort")
      //System.clearProperty("spark.ui.port")
    }
  }

  private def availablePort(): Int = {
    // TODO: we could actually look for an available port
    RandomUtils.nextInt(10000, 30000)
  }
}
