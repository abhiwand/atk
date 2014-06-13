package com.intel.intelanalytics.service

import com.intel.intelanalytics.repository.{Profile, DbProfileComponent, SlickMetaStoreComponent}
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.domain.UserTemplate

/**
 * A MetaStore that has been configured to be used
 */
class MetaStoreConfigured extends SlickMetaStoreComponent with DbProfileComponent {

  override lazy val profile = Profile.initializeFromConfig()

  if (profile.createTables) {
    val config = ConfigFactory.load()
    //populate the database with some test users from the specified file (for testing)
    val usersFile = config.getString("intel.analytics.test.users.file")
    //read from the resources folder
    val source = scala.io.Source.fromURL(getClass.getResource("/" + usersFile))

    try {
      metaStore.createAllTables()

      metaStore.withSession("Populating test users") {
        implicit session =>
          for (line <- source.getLines() if !line.startsWith("#")) {
            val cols: Array[String] = line.split(",")
            val apiKey = cols(1).trim
            info(s"Creating test user with api key $apiKey")
            metaStore.userRepo.insert(new UserTemplate(apiKey)).get
            assert(metaStore.userRepo.scan().length > 0, "No user was created")
            assert(metaStore.userRepo.retrieveByColumnValue("api_key", apiKey).length == 1, "User not found by api key")
          }
      }
    }
    finally {
      source.close()
    }
  }
}
