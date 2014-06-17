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