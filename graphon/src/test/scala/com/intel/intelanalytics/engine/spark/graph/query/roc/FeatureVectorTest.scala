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

package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.elements.{ Property, GBVertex }
import org.scalatest.matchers.{ MatchResult, Matcher }
import org.scalatest.{ FlatSpec, Matchers }
import com.intel.testutils.MatcherUtils._

object FeatureVectorTest {

}
class FeatureVectorTest extends FlatSpec with Matchers {
  val tolerance = 0.001

  "FeatureVector" should "parse graph elements" in {
    val vertex = GBVertex(1, Property("gbID", 1),
      Set(Property("prior", "0.1 0.2"), Property("posterior", "0.4,0.6"), Property("split", "TR")))
    val featureVector = FeatureVector.parseGraphElement(vertex, "prior", Some("posterior"), Some("split"))

    featureVector.priorArray should equalWithTolerance(Array(0.1, 0.2), tolerance)
    featureVector.posteriorArray should equalWithTolerance(Array(0.4, 0.6), tolerance)
    featureVector.splitType shouldEqual ("TR")

  }

}
