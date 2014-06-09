package com.intel.intelanalytics.engine

import org.scalatest.Matchers
import org.apache.spark.engine.SparkProgressListener
import java.util.concurrent.Semaphore

class SparkJobConcurrencyTest  extends TestingSparkContext with Matchers {
  "Running multiple thread" should "keep isolation between threads when setting properties" in {
    val listener = new SparkProgressListener()
//    sc.addSparkListener(listener)
    val sem = new Semaphore(0)
    val num = 10
    (1 to num).map {
      i => new Thread() {
        override def run() {
          sc.setLocalProperty("test", i.toString)
          assert(sc.getLocalProperty("test") === i.toString)
          sem.release()
        }
      }
    }

    sem.acquire(num)
    assert(sc.getLocalProperty("test") === null)
  }




}
