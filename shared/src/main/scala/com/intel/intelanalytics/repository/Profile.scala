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

package com.intel.intelanalytics.repository

import scala.slick.driver.{PostgresDriver, H2Driver, JdbcProfile}
import com.intel.intelanalytics.shared.SharedConfig

/**
 * Profiles are how we abstract various back-ends like H2 vs. PostgreSQL
 *
 * @param profile Slick profile
 * @param connectionString JDBC connection string
 * @param driver JDBC driver to use
 * @param username database user (should be empty string for H2)
 * @param password database password (should be empty string for H2)
 */
case class Profile(profile: JdbcProfile,
                   connectionString: String,
                   driver: String,
                   username: String = "",
                   password: String = "") {

  /**
   * True if database is H2, False otherwise.
   *
   * With H2 it makes sense to initialize the schema differently.
   */
  val isH2: Boolean = profile match {
    case H2Driver => true
    case _ => false
  }
}

object Profile {

  /**
   * Initialize a Profile from settings in the config
   */
  def initializeFromConfig(config: SharedConfig): Profile = {

    val driver = config.metaStoreConnectionDriver

    new Profile(jdbcProfileForDriver(driver),
                connectionString = config.metaStoreConnectionUrl,
                driver,
                username = config.metaStoreConnectionUsername,
                password = config.metaStoreConnectionPassword)
  }

  /**
   * Initialize the JdbcProfile based on the Driver name
   * @param driver jdbcDriver name, e.g. "org.h2.Driver"
   * @return the correct JdbcProfile
   */
  def jdbcProfileForDriver(driver: String): JdbcProfile = driver match {
    case "org.h2.Driver" => H2Driver
    case "org.postgresql.Driver" => PostgresDriver
    case _ => throw new IllegalArgumentException("Driver not supported: " + driver)

  }
}