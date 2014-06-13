package com.intel.intelanalytics.repository

import scala.slick.driver.{PostgresDriver, H2Driver, JdbcProfile}
import com.typesafe.config.ConfigFactory

/**
 * Profiles are how we abstract various back-ends like H2 vs. PostgreSQL
 *
 * @param profile Slick profile
 * @param connectionString JDBC connection string
 * @param driver JDBC driver to use
 * @param username database user (not needed for H2)
 * @param password database password (not needed for H2)
 * @param createTables true to create the underlying DDL needed
 */
case class Profile(profile: JdbcProfile,
                   connectionString: String,
                   driver: String,
                   username: String = null,
                   password: String = null,
                   createTables: Boolean = false)

object Profile {

  /**
   * Initialize a Profile from settings in the config
   */
  def initializeFromConfig(): Profile = {

    val config = ConfigFactory.load()
    val connectionString = config.getString("intel.analytics.metastore.connection.url")
    val driver = config.getString("intel.analytics.metastore.connection.driver")
    val username = config.getString("intel.analytics.metastore.connection.username")
    val password = config.getString("intel.analytics.metastore.connection.password")
    val createTables = config.getBoolean("intel.analytics.metastore.connection.createTables")

    new Profile(jdbcProfileForDriver(driver), connectionString, driver, username, password, createTables)
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