package com.intel.intelanalytics.engine.spark

import org.scalatest.Matchers
import org.apache.spark.engine.SparkProgressListener
import java.util.concurrent.Semaphore
import com.intel.intelanalytics.engine.ProgressInfo
import java.io.File
import org.apache.commons.io.FileUtils
import com.intel.testutils.TestingSparkContextFlatSpec

class SparkJobConcurrencyTest extends TestingSparkContextFlatSpec with Matchers {
  "Running multiple thread" should "keep isolation between threads when setting properties" in {

    val updater = new CommandProgressUpdater {
      override def updateProgress(commandId: Long, progress: List[ProgressInfo]): Unit = {
        //do nothing
      }
    }

    val listener = new SparkProgressListener(updater)
    sparkContext.addSparkListener(listener)

    def createTempDir: File = {
      val file = File.createTempFile("test", "-tmp")
      val path = file.getAbsolutePath
      file.delete()
      if (!file.mkdirs()) {
        throw new RuntimeException("Failed to create tmpDir: " + path)
      }
      file
    }
    val file = createTempDir
    val path = file.getAbsolutePath

    val sem = new Semaphore(0)
    //There is a bug in org.apache.hadoop.conf.Configuration
    //which will throws java.util.ConcurrentModificationException non-deterministically
    //issue https://issues.apache.org/jira/browse/HADOOP-10456
    //TODO: set num to 100 when we get a version of Spark that use a newer version of hadoop library which contains the fix

    val num = 1 //100
    val threads = (1 to num).map {
      i =>
        new Thread() {
          override def run() {
            sparkContext.setLocalProperty("command-id", i.toString)
            val carOwnerShips = List(Array[Any]("Bob", "Mustang,Camry"), Array[Any]("Josh", "Neon,CLK"), Array[Any]("Alice", "PT Cruiser,Avalon,F-150"), Array[Any]("Tim", "Beatle"), Array[Any]("Becky", ""))
            val rdd = sparkContext.parallelize(carOwnerShips)
            val flattened = SparkOps.flattenRddByColumnIndex(1, ",", rdd)
            flattened.saveAsTextFile(new File(path, "command-" + i.toString).toString)
            sem.release()
          }
        }
    }

    threads.foreach(_.start())

    sem.acquire(num)

    val commandIds = listener.commandIdJobs.map {
      t => t._2(0).properties.getProperty("command-id").toInt
    }

    val distinctIds = commandIds.toSet.toList.sorted

    distinctIds.size shouldBe num
    //    for(i <- distinctIds)
    //      println("######## command" + i)
    FileUtils.deleteQuietly(file)
  }

}
