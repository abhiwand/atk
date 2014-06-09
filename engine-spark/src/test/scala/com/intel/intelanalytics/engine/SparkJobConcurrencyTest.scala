package com.intel.intelanalytics.engine.spark

import org.scalatest.Matchers
import org.apache.spark.engine.{ProgressPrinter, SparkProgressListener}
import java.util.concurrent.Semaphore
import com.intel.intelanalytics.engine.spark.SparkOps
import com.intel.intelanalytics.engine.TestingSparkContext

class SparkJobConcurrencyTest  extends TestingSparkContext with Matchers {
  "Running multiple thread" should "keep isolation between threads when setting properties" in {
    val listener = new SparkProgressListener()
    sc.addSparkListener(listener)
    val sem = new Semaphore(0)
    val num = 100
    val threads = (1 to num).map {
      i => new Thread() {
        override def run() {
          sc.setLocalProperty("command-id", i.toString)
          val carOwnerShips = List(Array[Any]("Bob", "Mustang,Camry"), Array[Any]("Josh", "Neon,CLK"), Array[Any]("Alice", "PT Cruiser,Avalon,F-150"), Array[Any]("Tim", "Beatle"), Array[Any]("Becky", ""))
          val rdd = sc.parallelize(carOwnerShips)
          val flattened = SparkOps.flattenRddByColumnIndex(1, ",", rdd)
          flattened.saveAsTextFile("command-" + i.toString)
          sem.release()
        }
      }
    }

    threads.foreach(_.start())

    sem.acquire(num)


    val commandIds = listener.commandIdJobs.map {
      t => t._2.properties.getProperty("command-id").toInt
    }

    val distinctIds = commandIds.toSet.toList.sorted

    distinctIds.size shouldBe num
    for(i <- distinctIds)
      println("######## command" + i)
  }




}
