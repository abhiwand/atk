//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.service

import com.intel.intelanalytics.repository.{Profile, DbProfileComponent, SlickMetaStoreComponent}
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.domain.UserTemplate

/**
 * A MetaStore that has been configured to be used
 */
class MetaStoreConfigured extends SlickMetaStoreComponent with DbProfileComponent {

  override lazy val profile = Profile.initializeFromConfig(ApiServiceConfig)

  if (profile.createTables) {

    //populate the database with some test users from the specified file (for testing), read from the resources folder
    val source = scala.io.Source.fromURL(getClass.getResource("/" + ApiServiceConfig.testUsersFile))

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
