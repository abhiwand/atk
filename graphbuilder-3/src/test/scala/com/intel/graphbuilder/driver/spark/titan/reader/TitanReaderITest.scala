//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanReaderRdd
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReaderTestData._
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge, GraphElement }
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
      val titanReaderRDD = new TitanReaderRdd(hBaseRDD, titanConnector)
      val graphElements = titanReaderRDD.collect()
      graphElements.length shouldBe 0
    }
    "return 3 GraphBuilder vertices and 2 GraphBuilder rows" in {
      val hBaseRDD = sparkContext.parallelize(
        Seq((NullWritable.get(), neptuneFaunusVertex),
          (NullWritable.get(), plutoFaunusVertex),
          (NullWritable.get(), seaFaunusVertex)))

      val titanReaderRDD = new TitanReaderRdd(hBaseRDD, titanConnector)
      val vertexRDD = titanReaderRDD.filterVertices()
      val edgeRDD = titanReaderRDD.filterEdges()

      val graphElements = titanReaderRDD.collect()
      val vertices = vertexRDD.collect()
      val edges = edgeRDD.collect()

      graphElements.length shouldBe 5
      vertices.length shouldBe 3
      edges.length shouldBe 2

      graphElements.map(e => e match {
        case v: GBVertex => v
        case e: GBEdge => e.copy(eid = None)
      }) should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex, plutoGbEdge, seaGbEdge)
      vertices should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex)
      edges.map(e => e.copy(eid = None)) should contain theSameElementsAs List[GraphElement](plutoGbEdge, seaGbEdge)
    }
  }
}
