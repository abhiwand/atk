package com.intel.intelanalytics.repository

import org.scalatest.Matchers
import com.intel.intelanalytics.domain.UserTemplate

class SlickMetaStoreComponentSpec extends SlickMetaStoreH2Testing with Matchers {

  "MetaStore" should "be able to initialize tables" in {
    val results = slickMetaStoreComponent.database.createConnection().getMetaData.getTables(null, null, null, Array("TABLE"))
    var count = 0
    while (results.next()) {
      count += 1
    }
    count shouldEqual 4
  }

  "UserRepository" should "be able to create users" in {
    val userRepo = slickMetaStoreComponent.metaStore.userRepo

    slickMetaStoreComponent.metaStore.withSession("user-test") {
      implicit session =>
        val apiKey = "my-api-key-" + System.currentTimeMillis()

        val user = userRepo.insert(new UserTemplate(apiKey))
        user.get.api_key shouldBe apiKey

        val user2 = userRepo.lookup(user.get.id)
        user.get shouldBe user2.get
    }
  }
}
