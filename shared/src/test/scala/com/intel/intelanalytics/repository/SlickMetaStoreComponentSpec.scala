package com.intel.intelanalytics.repository

import org.scalatest.{Matchers, FlatSpec}
import scala.slick.driver.H2Driver

class SlickMetaStoreComponentSpec extends FlatSpec with Matchers {

  val slickMetaStoreComponent = new SlickMetaStoreComponent with DbProfileComponent {
    override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:iatest;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
  }

  "MetaStore" should "be able to initialize tables" in {
    slickMetaStoreComponent.metaStore.create()

    val results = slickMetaStoreComponent.database.createConnection().getMetaData.getTables(null, null, null, Array("TABLE"))
    var count = 0
    while (results.next()) {
      count += 1
    }
    count shouldEqual 3
  }
}
