package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReaderTestData._
import com.intel.graphbuilder.elements.GraphElement
import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.scalatest.{ Matchers, WordSpec }

import scala.collection.JavaConversions._

/**
 * End-to-end integration test for Titan reader
 * @todo Use Stephen's TestingSparkContext class for scalatest
 */
class TitanReaderITest extends TestingSparkContextWordSpec with Matchers {

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

      graphElements should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex, plutoGbEdge, seaGbEdge)
      vertices should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex)
      edges should contain theSameElementsAs List[GraphElement](plutoGbEdge, seaGbEdge)
    }
  }
}

