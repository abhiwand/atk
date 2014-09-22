package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReaderTestData._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReaderUtils.sortGraphElementProperties
import com.intel.graphbuilder.elements.GraphElement
import com.intel.testutils.TestingSparkContextWordSpec
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.NullWritable
import org.scalatest.{ Matchers, WordSpec }

import scala.collection.JavaConversions._

/**
 * End-to-end integration test for Titan reader
 * @todo Use Stephen's TestingSparkContext class for scalatest
 */
class TitanReaderITest extends TestingSparkContextWordSpec with Matchers {

  "Reading a Titan graph from HBase should" should {
    "return an empty list of graph elements if the HBase table is empty" in {
      val hBaseRDD = sparkContext.parallelize(Seq.empty[(NullWritable, FaunusVertex)])
      val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector)
      val graphElements = titanReaderRDD.collect()
      graphElements.length shouldBe 0
    }
    "return 3 GraphBuilder vertices and 2 GraphBuilder rows" in {
      val hBaseRDD = sparkContext.parallelize(
        Seq((NullWritable.get(), neptuneFaunusVertex),
        (NullWritable.get(), plutoFaunusVertex),
        (NullWritable.get(), seaFaunusVertex)))

      val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector)
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

