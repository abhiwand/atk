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

package com.intel.graphbuilder.driver.spark.rdd

import org.specs2.mutable.Specification
import com.intel.graphbuilder.testutils.TestingSparkContext
import com.intel.graphbuilder.elements.{ GbIdToPhysicalId, Property, Edge }
import GraphBuilderRDDImplicits._

class EdgeRDDFunctionsITest extends Specification {

  "EdgeRDDFunctions" should {

    // A lot of tests are being grouped together here because it
    // is somewhat expensive to spin up a testing SparkContext
    "pass integration test" in new TestingSparkContext {

      val edge1 = new Edge(Property("gbId", 1L), Property("gbId", 2L), "myLabel", List(Property("key", "value")))
      val edge2 = new Edge(Property("gbId", 2L), Property("gbId", 3L), "myLabel", List(Property("key", "value")))
      val edge3 = new Edge(Property("gbId", 1L), Property("gbId", 2L), "myLabel", List(Property("key2", "value2")))

      val gbIdToPhysicalId1 = new GbIdToPhysicalId(Property("gbId", 1L), new java.lang.Long(1001L))
      val gbIdToPhysicalId2 = new GbIdToPhysicalId(Property("gbId", 2L), new java.lang.Long(1002L))

      val edges = sc.parallelize(List(edge1, edge2, edge3))
      val gbIdToPhysicalIds = sc.parallelize(List(gbIdToPhysicalId1, gbIdToPhysicalId2))

      val merged = edges.mergeDuplicates()
      merged.count() mustEqual 2

      val biDirectional = edges.biDirectional()
      biDirectional.count() mustEqual 6

      val mergedBiDirectional = biDirectional.mergeDuplicates()
      mergedBiDirectional.count() mustEqual 4

      val filtered = biDirectional.filterEdgesWithoutPhysicalIds()
      filtered.count() mustEqual 0

      val joined = biDirectional.joinWithPhysicalIds(gbIdToPhysicalIds)
      joined.count() mustEqual 4
      joined.filterEdgesWithoutPhysicalIds().count() mustEqual 4

      val vertices = edges.verticesFromEdges()
      vertices.count() mustEqual 6
      vertices.mergeDuplicates().count() mustEqual 3

    }
  }

}

