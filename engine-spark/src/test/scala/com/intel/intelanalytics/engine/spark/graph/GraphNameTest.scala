package com.intel.intelanalytics.engine.spark.graph

import org.scalatest.{ Matchers, WordSpec }

class GraphNameTest extends WordSpec with Matchers {

  "ConvertGraphUserNameToBackendName" should {
    "preface user name with expected string" in {
      val userName = "Graph of the Gods"
      val expectedBackendName = "iat_graph_" + userName
      GraphBackendName.convertGraphUserNameToBackendName(userName) shouldEqual expectedBackendName
    }
  }

  "ConvertGraphBackendNameToUserName" should {
    "strip expected string from the backend name" in {
      val expectedUserName = "VALHALLA! I AM COMING!!!"
      val backendName = "iat_graph_" + expectedUserName
      GraphBackendName.convertGraphBackendNameToUserName(backendName) shouldEqual expectedUserName
    }
  }
}
