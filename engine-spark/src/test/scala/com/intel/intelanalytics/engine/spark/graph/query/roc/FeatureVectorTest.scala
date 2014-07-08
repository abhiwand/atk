package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.elements.{ Property, Vertex }
import org.scalatest.matchers.{ MatchResult, Matcher }
import org.scalatest.{ FlatSpec, Matchers }
import com.intel.testutils.MatcherUtils._

object FeatureVectorTest {

}
class FeatureVectorTest extends FlatSpec with Matchers {
  val tolerance = 0.001

  "FeatureVector" should "parse graph elements" in {
    val vertex = Vertex(1, Property("gbID", 1),
      Seq(Property("prior", "0.1,0.2"), Property("posterior", "0.4,0.6"), Property("split", "TR")))
    val featureVector = FeatureVector.parseGraphElement(vertex, "prior", Some("posterior"), Some("split")).get

    featureVector.priorArray should equalWithTolerance(Array(0.1, 0.2), tolerance)
    featureVector.posteriorArray should equalWithTolerance(Array(0.4, 0.6), tolerance)
    featureVector.splitType shouldEqual ("TR")

  }
  it should "parse doubles" in {
    FeatureVector.parseDouble("0.1").get should be(0.1 +- tolerance)
    FeatureVector.parseDouble(" 12.2 ").get should be(12.2 +- tolerance) //Checks double with spaces
    FeatureVector.parseDouble("-100.55").get should be(-100.55 +- tolerance)
  }

}
