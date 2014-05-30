package com.intel.graphbuilder.driver.spark.titan.reader

import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.{SparkException, SparkContext, SparkConf}
import java.util.Date
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.elements.GraphElement
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.CellUtil
import scala.collection.JavaConversions._
import com.intel.graphbuilder.testutils.TitanReaderTestData

class TitanReaderItest extends WordSpec with Matchers {

  import TitanReaderTestData._

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName(this.getClass.getSimpleName + " " + new Date())
    .setSparkHome(ExamplesUtils.sparkHome)
    .setJars(List(ExamplesUtils.gbJar))
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")

  val sc = new SparkContext(conf)

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

      graphElements should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex, plutoGbEdge, seaGbEdge)
      vertices should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex)
      edges should contain theSameElementsAs List[GraphElement](plutoGbEdge, seaGbEdge)
    }
  }

}
