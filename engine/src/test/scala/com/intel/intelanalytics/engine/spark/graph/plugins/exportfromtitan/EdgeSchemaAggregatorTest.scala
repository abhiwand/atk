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

package com.intel.intelanalytics.engine.spark.graph.plugins.exportfromtitan

import com.intel.graphbuilder.elements.{ Property, GBEdge }
import org.scalatest.WordSpec
import EdgeSchemaAggregator.zeroValue

class EdgeSchemaAggregatorTest extends WordSpec {

  val edge1 = GBEdge(Some(3L), 5L, 6L, Property("movieId", 8L), Property("userId", 9L), "rating", Set(Property("rating", 6)))
  val edgeHolder1 = EdgeHolder(edge1, "movies", "users")

  "EdgeSchemaAggregator" should {

    "be able to get schema from edges" in {
      val edgeSchema = EdgeSchemaAggregator.toSchema(edgeHolder1)

      assert(edgeSchema.label == "rating")
      assert(edgeSchema.srcVertexLabel == "movies")
      assert(edgeSchema.destVertexLabel == "users")
      assert(edgeSchema.hasColumn("rating"))
      assert(edgeSchema.columns.size == 5, "4 edge system columns plus one user defined column")
    }

    "combining zero values should still be a zero value" in {
      assert(EdgeSchemaAggregator.combOp(zeroValue, zeroValue) == zeroValue)
    }

    "aggregating one edge more than once should not change the output" in {
      val agg1 = EdgeSchemaAggregator.seqOp(zeroValue, edgeHolder1)
      val agg2 = EdgeSchemaAggregator.seqOp(agg1, edgeHolder1)
      assert(agg1 == agg2)

      val agg3 = EdgeSchemaAggregator.combOp(agg1, agg2)
      assert(agg1 == agg3)
    }
  }
}
