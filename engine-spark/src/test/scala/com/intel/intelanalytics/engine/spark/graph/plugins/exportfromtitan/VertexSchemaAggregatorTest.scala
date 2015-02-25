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

import com.intel.graphbuilder.elements.{ Property, GBVertex }
import org.scalatest.{ WordSpec, FunSuite }

class VertexSchemaAggregatorTest extends WordSpec {

  val vertexSchemaAggregator = new VertexSchemaAggregator(List("movieId"))

  "VertexSchemaAggregator" should {
    "convert GBVertices to VertexSchemas" in {
      val schema = vertexSchemaAggregator.toSchema(GBVertex(1L, Property("titanPhysicalId", 2L), Set(Property("_label", "movie"), Property("movieId", 23L))))
      assert(schema.label == "movie")
      assert(schema.columns.size == 3, "expected _vid, _label, and movieId")
    }
  }

}
