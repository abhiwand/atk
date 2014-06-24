package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.UserTemplate
import org.scalatest.Matchers

class UserRespositorySpec extends SlickMetaStoreH2Testing with Matchers {

  "UserRepository" should "be able to create users" in {

    val userRepo = slickMetaStoreComponent.metaStore.userRepo

    slickMetaStoreComponent.metaStore.withSession("user-test") {
      implicit session =>

        val apiKey = "my-api-key-" + System.currentTimeMillis()

        // create a user
        val user = userRepo.insert(new UserTemplate(apiKey))
        user.get.api_key shouldBe apiKey

        // look it up and validate expected values
        val user2 = userRepo.lookup(user.get.id)
        user.get shouldBe user2.get
        user.get.createdOn should not be null
        user.get.modifiedOn should not be null
    }
  }
}
