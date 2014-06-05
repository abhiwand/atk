package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.testutils.{TestingSparkContext, LogUtils}
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.elements.GraphElement
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.CellUtil
import org.scalatest.{WordSpec,Matchers,BeforeAndAfterAll}
import scala.collection.JavaConversions._
import java.util.Date

import TitanReaderTestData._
import TitanReaderUtils.sortGraphElementProperties


/**
 * End-to-end integration test for Titan reader
 * @todo Use Stephen's TestingSparkContext class for scalatest
 */
class TitanReaderITest extends WordSpec with Matchers with TitanReaderSparkContext {

  "Reading a Titan graph from HBase should" should {
    "return an empty list of graph elements if the HBase table is empty" in {
      val hBaseRDD = sparkContext.parallelize(Seq.empty[(ImmutableBytesWritable, Result)])
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

        val invalidHBaseRow = List((new ImmutableBytesWritable(rowKey), Result.create(invalidHBaseCell))).toSeq
        val hBaseRDD = sparkContext.parallelize(invalidHBaseRow)
        val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector)

        titanReaderRDD.collect()
      }
    }
    "return 3 GraphBuilder vertices and 2 GraphBuilder rows" in {
      val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

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

trait TitanReaderSparkContext extends WordSpec with BeforeAndAfterAll {
  LogUtils.silenceSpark()

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getSimpleName + " " + new Date())
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")

  var sparkContext: SparkContext = null

  override def beforeAll = {
    // Ensure only one Spark local context is running at a time
    TestingSparkContext.lock.acquire()
    sparkContext = new SparkContext(conf)
  }

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
      if (sparkContext != null) {
        sparkContext.stop()
      }
    }
    finally {
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
      TestingSparkContext.lock.release()
    }
  }
}

