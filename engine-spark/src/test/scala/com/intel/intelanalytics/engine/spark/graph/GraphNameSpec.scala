package com.intel.intelanalytics.engine.spark.graph

import org.specs2.mutable.Specification

class GraphNameSpec extends Specification {

  "ConvertGraphUserNameToBackendName" should {
    "preface user name with expected string" in {
      val userName = "Graph of the Gods"
      val expectedBackendName = "iat_graph_" + userName
      GraphName.convertGraphUserNameToBackendName(userName) shouldEqual
        expectedBackendName
    }
  }

  "ConvertGraphBackendNameToUserName" should {
    "strip expected string from the backend name" in {
      val expectedUserName = "VALHALLA! I AM COMING!!!"
      val backendName = "iat_graph_" + expectedUserName
      GraphName.convertGraphBackendNameToUserName(backendName) shouldEqual expectedUserName
    }
  }
}
