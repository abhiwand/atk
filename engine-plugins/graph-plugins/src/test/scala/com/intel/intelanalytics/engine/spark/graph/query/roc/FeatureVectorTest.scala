/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
