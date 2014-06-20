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

package com.intel.intelanalytics.service.v1.decorators

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.intel.intelanalytics.domain.graph.Graph
import org.joda.time.DateTime

class GraphDecoratorSpec extends FlatSpec with Matchers {

  val uri = "http://www.example.com/graphs"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val graph = new Graph(1, "name", None, "storage", 1L, new DateTime, new DateTime)

  "GraphDecorator" should "be able to decorate a graph" in {
    val decoratedGraph = GraphDecorator.decorateEntity(null, relLinks, graph)
    decoratedGraph.id should be(1)
    decoratedGraph.name should be("name")
    decoratedGraph.links.head.uri should be("http://www.example.com/graphs")
  }

  it should "set the correct URL in decorating a list of graphs" in {
    val graphHeaders = GraphDecorator.decorateForIndex(uri, Seq(graph))
    val graphHeader = graphHeaders.toList.head
    graphHeader.url should be("http://www.example.com/graphs/1")
  }
}
