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
import com.intel.graphbuilder.elements._
import GraphBuilderRDDImplicits._
import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.elements.Edge

class GraphElementRDDFunctionsITest extends Specification {

  "GraphElementRDDFunctions" should {

    // A lot of tests are being grouped together here because it
    // is somewhat expensive to spin up a testing SparkContext
    "pass integration test" in new TestingSparkContext {

      val edge1 = new Edge(Property("gbId", 1L), Property("gbId", 2L), "myLabel", List(Property("key", "value")))
      val edge2 = new Edge(Property("gbId", 2L), Property("gbId", 3L), "myLabel", List(Property("key", "value")))

      val vertex = new Vertex(Property("gbId", 2L), Nil)

      val graphElements = sc.parallelize(List[GraphElement](edge1, edge2, vertex))

      graphElements.filterEdges().count() mustEqual 2
      graphElements.filterVertices().count() mustEqual 1
    }
  }
}


