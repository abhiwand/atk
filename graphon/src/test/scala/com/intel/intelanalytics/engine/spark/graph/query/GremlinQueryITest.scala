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

package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.testutils.MatcherUtils._
import com.intel.testutils.TestingTitan
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import spray.json.JsNumber

class GremlinQueryITest extends FlatSpec with Matchers with TestingTitan with BeforeAndAfter {
  before {
    setupTitan()
  }

  after {
    cleanupTitan()
  }

  "executeGremlinQuery" should "execute valid Gremlin queries" in {
    val vertex1 = titanGraph.addVertex(null)
    val vertex2 = titanGraph.addVertex(null)
    val edge = titanGraph.addEdge(null, vertex1, vertex2, "knows")

    vertex1.setProperty("name", "alice")
    vertex1.setProperty("age", 23)
    vertex2.setProperty("name", "bob")
    vertex2.setProperty("age", 27)

    titanGraph.commit()

    val gremlinQuery = new GremlinQuery()
    val gremlinScript = """g.V("name", "alice").out("knows")"""

    val bindings = gremlinQuery.gremlinExecutor.createBindings()
    bindings.put("g", titanGraph)

    val results = gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    val vertexCount = gremlinQuery.executeGremlinQuery(titanGraph, "g.V.count()", bindings).toArray
    val edgeCount = gremlinQuery.executeGremlinQuery(titanGraph, "g.E.count()", bindings).toArray

    results.size should equal(1)
    vertexCount.size should equal(1)
    edgeCount.size should equal(1)

    vertex2 should equalsGraphSONVertex(results(0))
    vertexCount(0) should equal(JsNumber(2))
    edgeCount(0) should equal(JsNumber(1))
  }
  "executeGremlinQuery" should "throw a Runtime exception when executing invalid Gremlin" in {
    intercept[java.lang.RuntimeException] {
      val gremlinQuery = new GremlinQuery()
      val gremlinScript = """InvalidGremlin"""

      val bindings = gremlinQuery.gremlinExecutor.createBindings()
      bindings.put("g", titanGraph)

      gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    }
  }

}
