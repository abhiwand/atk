package com.intel.graphbuilder.testutils

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock

/**
 * This trait case be mixed into Specifications to create a SparkContext for testing.
 * <p>
 * IMPORTANT! This adds a couple seconds to your unit test!
 * </p>
 * <p>
 * Lock is used because you can only have one local SparkContext running at a time.
 * Other option is to use "parallelExecution in Test := false" but locking seems to be faster.
 * </p>
 */
trait TestingSparkContext extends MultipleAfter {

  // locking in the constructor is slightly odd but it seems to work well
  TestingSparkContext.lock.acquire()

  LogUtils.silenceSpark()

  lazy val sc = new SparkContext("local", "test " + new Date())

  /**
   * Clean up after the test is done
   */
  override def after: Any = {
    cleanupSpark()
    super.after
  }

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