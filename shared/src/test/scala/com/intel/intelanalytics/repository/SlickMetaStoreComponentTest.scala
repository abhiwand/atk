package com.intel.intelanalytics.repository

import org.scalatest.Matchers

class SlickMetaStoreComponentTest extends SlickMetaStoreH2Testing with Matchers {

  "MetaStore" should "be able to initialize tables" in {
    val results = slickMetaStoreComponent.database.createConnection().getMetaData.getTables(null, null, null, Array("TABLE"))
    var count = 0
    while (results.next()) {
      count += 1
    }
    count shouldEqual 7 // expected number of tables in the meta store
  }

}
