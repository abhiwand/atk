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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.Call
import org.scalatest.{ FlatSpec, Matchers }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.intel.intelanalytics.domain.schema.{ Column, VertexSchema }
import com.intel.intelanalytics.domain.schema.DataTypes.{ string, int64 }

class ExportToGraphPluginITest extends FlatSpec with Matchers with MockitoSugar {
  implicit val call = Call(null)

  "createVertexFrames" should "vertex frame by label" in {
    val graphs = mock[SparkGraphStorage]
    val graphRef = GraphReference(1)
    ExportToGraphPlugin.createVertexFrames(graphs, graphRef, List("user", "movie"))

    verify(graphs).defineVertexType(graphRef, VertexSchema(List(Column("_vid", int64), Column("_label", string)), label = "user", idColumnName = None))
    verify(graphs).defineVertexType(graphRef, VertexSchema(List(Column("_vid", int64), Column("_label", string)), label = "movie", idColumnName = None))
  }
}
