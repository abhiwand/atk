package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.elements.GraphElement
import TitanReaderUtils.sortGraphElementProperties
import org.scalatest.{ Suite, BeforeAndAfterAll, Matchers, WordSpec }
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.CellUtil
import scala.collection.JavaConversions._
import scala.concurrent.Lock
import java.util.Date
import com.intel.graphbuilder.testutils.LogUtils

/**
 * End-to-end integration test for Titan reader
 * @todo Use Stephen's TestingSparkContext class for scalatest
 */
class TitanReaderITest extends WordSpec with Matchers {

  import TitanReaderTestData._
  import TitanReaderSparkContext._

  "Reading a Titan graph from HBase should" should {
    "return an empty list of graph elements if the HBase table is empty" in {
      val hBaseRDD = sc.parallelize(Seq.empty[(ImmutableBytesWritable, Result)])
      val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector)
      val graphElements = titanReaderRDD.collect()
      graphElements.length shouldBe 0
    }
    "throw an Exception when a HBase row is not serialized in a format that Titan understands" in {
      intercept[Exception] {
        val rowKey = "key".getBytes()
        val dummyTimestamp = 1
        val dummyType = 0.toByte

        val invalidHBaseCell = List(CellUtil.createCell(
          rowKey,
          "columnfamily".getBytes(),
          "qualifier".getBytes(),
          dummyTimestamp, dummyType,
          "value".getBytes()))

        val invalidHBaseRow = List((new ImmutableBytesWritable(rowKey), Result.create(invalidHBaseCell)))
        val hBaseRDD = sc.parallelize(invalidHBaseRow)
        val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector)

        titanReaderRDD.collect()
      }
    }
    "return 3 GraphBuilder vertices and 2 GraphBuilder rows" in {
      val hBaseRDD = sc.parallelize(hBaseRowMap.toSeq)

      val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
      val vertexRDD = titanReaderRDD.filterVertices()
      val edgeRDD = titanReaderRDD.filterEdges()

      val graphElements = titanReaderRDD.collect()
      val vertices = vertexRDD.collect()
      val edges = edgeRDD.collect()

      graphElements.length shouldBe 5
      vertices.length shouldBe 3
      edges.length shouldBe 2

      val sortedGraphElements = sortGraphElementProperties(graphElements)
      val sortedVertices = sortGraphElementProperties(vertices.asInstanceOf[Array[GraphElement]])
      val sortedEdges = sortGraphElementProperties(edges.asInstanceOf[Array[GraphElement]])

      sortedGraphElements should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex, plutoGbEdge, seaGbEdge)
      sortedVertices should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex)
      sortedEdges should contain theSameElementsAs List[GraphElement](plutoGbEdge, seaGbEdge)
    }
  }
}

object TitanReaderSparkContext extends Suite with BeforeAndAfterAll {
  LogUtils.silenceSpark()
  val lock = new Lock()
  lock.acquire()

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getSimpleName + " " + new Date())
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
  val sc = new SparkContext(conf)

  /**
   * Clean up after the test is done
   */
  override def afterAll = {
    cleanupSpark()
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

      lock.release()
    }
  }
}

