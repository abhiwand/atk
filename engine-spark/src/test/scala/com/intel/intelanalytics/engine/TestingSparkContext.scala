package com.intel.intelanalytics.engine

import org.apache.spark.SparkContext
import java.util.Date
import scala.concurrent.Lock

trait TestingSparkContext {

  lazy val sc = new SparkContext("local", "test " + new Date())

  /**
   * Shutdown spark and release the lock
   */
  def cleanupSpark(): Unit = {
    try {
      if (sc != null) {
        sc.stop()
      }
    }
    finally {
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")

      TestingSparkContext.lock.release()
    }
  }

}

object TestingSparkContext {
  val lock = new Lock()
}
