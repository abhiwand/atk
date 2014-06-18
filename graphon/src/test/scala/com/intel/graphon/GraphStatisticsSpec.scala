//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.graphon

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphon.TitanReaderTestData._
import com.intel.spark.graphon.GraphStatistics
import org.scalatest.{Matchers, WordSpec}

class GraphStatisticsSpec extends WordSpec with Matchers with TitanSparkContext {

  "Out degrees of vertices in test graph" in {
    val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

    val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()
    GraphStatistics.outDegrees(edgeRDD).collect().length shouldBe 1
  }

  "In degrees of vertices in test graph" in {
    val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

    val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()
    GraphStatistics.inDegrees(edgeRDD).collect().length shouldBe 2
  }

  "Out degrees of vertices with \"LIKES\" edge-type in test graph" in {
    val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

    val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()
    GraphStatistics.outDegreesByEdgeType(edgeRDD, "LIKES").collect().length shouldBe 0
  }

  "In degrees of vertices with \"LIKES\" edge-type in test graph" in {
    val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

    val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()
    GraphStatistics.inDegreesByEdgeType(edgeRDD, "LIKES").collect().length shouldBe 0
  }

  "Out degrees of vertices with \"lives\" edge-type in test graph" in {
    val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

    val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()
    GraphStatistics.outDegreesByEdgeType(edgeRDD, "lives").collect().length shouldBe 1
  }

  "In degrees of vertices with \"lives\" edge-type in test graph" in {
    val hBaseRDD = sparkContext.parallelize(hBaseRowMap.toSeq)

    val titanReaderRDD = new TitanHBaseReaderRDD(hBaseRDD, titanConnector).distinct()
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()
    GraphStatistics.inDegreesByEdgeType(edgeRDD, "lives").collect().length shouldBe 1
  }
}
