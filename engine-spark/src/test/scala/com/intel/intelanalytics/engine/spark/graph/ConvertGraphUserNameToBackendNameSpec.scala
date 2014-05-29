package com.intel.intelanalytics.engine.spark.graph

import org.specs2.mutable.Specification

class ConvertGraphUserNameToBackendNameSpec extends Specification {

  "ConvertGraphUserNameToBackendName" should {
    "preface user name with expected string" in {
      val userName = "Graph of the Gods"
      GraphName.convertGraphUserNameToBackendName(userName) shouldEqual
        GraphName.iatGraphTablePrefix + userName
    }
  }

  "ConvertGraphBackendNameToUserName" should {
    "strip expected string from the backend name" in {
      val userName = "VALHALLA! I AM COMING!!!"
      val backendName = GraphName.iatGraphTablePrefix + userName
      GraphName.convertGraphBackendNameToUserName(backendName) shouldEqual userName
    }
  }
}
