package com.intel.spark.graphon.connectedcomponents

import org.scalatest.WordSpec
import org.scalatest.Matchers

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock
import org.scalatest.{FlatSpec, BeforeAndAfter}
import org.apache.log4j.{Logger, Level}

class NormalizeConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContext {



  "normalize connected components" should "find three components" in {
    val vertexList: List[Long] = List(4, 8, 12, 16, 20, 24, 28, 32)
    val components = Set(Set(8, 24, 32), Set(4, 16, 28))
    val originalVertexToComponent = List((8, 8), (24, 8), (32, 8), (4, 28), (16, 28), (28, 28), (12, 12))

    val verticesToComponentsRDD = sc.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizeOut = NormalizeConnectedComponents.normalize(verticesToComponentsRDD)

    normalizeOut._1 shouldEqual 3
  }

  "normalize connected components" should "have same number of reported components in" in {
    val vertexList: List[Long] = List(4, 8, 12, 16, 20, 24, 28, 32)
    val components = Set(Set(8, 24, 32), Set(4, 16, 28))
    val originalVertexToComponent = List((8, 8), (24, 8), (32, 8), (4, 28), (16, 28), (28, 28), (12, 12))

    val verticesToComponentsRDD = sc.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizeOut = NormalizeConnectedComponents.normalize(verticesToComponentsRDD)

    normalizeOut._1 shouldEqual normalizeOut._2.count()
  }
}

// TODO: get TestingSparkContext from a shared location

trait TestingSparkContext extends FlatSpec with BeforeAndAfter {

  val useGlobalSparkContext: Boolean = System.getProperty("useGlobalSparkContext", "false").toBoolean

  var sc: SparkContext = null

  before {
    if (useGlobalSparkContext) {
      sc = TestingSparkContext.sc
    }
    else {
      TestingSparkContext.lock.acquire()
      sc = TestingSparkContext.createSparkContext
    }
  }

  /**
   * Clean up after the test is done
   */
  after {
    if (!useGlobalSparkContext) cleanupSpark()
  }

  /**
   * Shutdown spark and release the lock
   */
  private def cleanupSpark(): Unit = {
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
  private val lock = new Lock()
  private lazy val sc: SparkContext = createSparkContext

  def createSparkContext: SparkContext = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
    new SparkContext("local", "test " + new Date())
  }

  private def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]): Unit = {
    loggers.foreach(loggerName => Logger.getLogger(loggerName).setLevel(level))
  }
}
