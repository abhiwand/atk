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

class EdgeSchemaAggregatorTest extends WordSpec {

  "EdgeSchemaAggregator" should {

    "be able to get schema from edges" in {
      val edge = GBEdge(Some(3L), 5L, 6L, Property("movieId", 8L), Property("userId", 9L), "rating", Set(Property("rating", 6)))
      val edgeHolder = EdgeHolder(edge, "movies", "users")
      val edgeSchema = EdgeSchemaAggregator.toSchema(edgeHolder)

      assert(edgeSchema.label == "rating")
      assert(edgeSchema.srcVertexLabel == "movies")
      assert(edgeSchema.destVertexLabel == "users")
      assert(edgeSchema.hasColumn("rating"))
      assert(edgeSchema.columns.size == 5, "4 edge system columns plus one user defined column")

    }
  }
}
