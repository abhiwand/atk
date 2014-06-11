package com.intel.spark.graphon.connectedcomponents

import org.scalatest.Matchers

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock
import org.scalatest.{FlatSpec, BeforeAndAfter}
import org.apache.log4j.{Logger, Level}

class NormalizeConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContext {


  trait ConnectedComponentsTest {
    val vertexList: List[Long] = List(4, 8, 12, 16, 20, 24, 28, 32)
    val originalEquivalenceClasses: Set[Set[Long]] = Set(Set(8, 24, 32), Set(4, 16, 28), Set(12))
    val originalVertexToComponent: List[(Long, Long)] = List((8.toLong, 8.toLong),
      (24.toLong, 8.toLong), (32.toLong, 8.toLong), (4.toLong, 28.toLong), (16.toLong, 28.toLong),
      (28.toLong, 28.toLong), (12.toLong, 12.toLong))
  }

  "normalize connected components" should "not have any gaps in the range of component IDs returned" in new ConnectedComponentsTest {


    val verticesToComponentsRDD = sc.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizeOut = NormalizeConnectedComponents.normalize(verticesToComponentsRDD, sc)

    val normalizeOutComponents = normalizeOut._2.map(x => x._2).distinct()

    normalizeOutComponents.map(_.toLong).collect().toSet shouldEqual (1.toLong to (normalizeOutComponents.count().toLong)).toSet
  }


  "normalize connected components" should "create correct equivalence classes" in new ConnectedComponentsTest {


    val verticesToComponentsRDD = sc.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizedCCROutput = NormalizeConnectedComponents.normalize(verticesToComponentsRDD, sc)

    val vertexComponentPairs = normalizedCCROutput._2.collect()

    val groupedVertexComponentPairs = vertexComponentPairs.groupBy(x => x._2).map(x => x._2)

    val setsOfPairs = groupedVertexComponentPairs.map(list => list.toSet)

    val equivalenceclasses = setsOfPairs.map(set => set.map(x => x._1)).toSet

    equivalenceclasses shouldEqual originalEquivalenceClasses
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

