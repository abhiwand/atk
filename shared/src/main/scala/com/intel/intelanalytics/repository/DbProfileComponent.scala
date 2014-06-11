package com.intel.intelanalytics.repository

import scala.slick.driver.JdbcProfile

trait DbProfileComponent {

  val profile: Profile

  /**
   * Profiles are how we abstract various back-ends like H2 vs. PostgreSQL
   */
  case class Profile(profile: JdbcProfile, connectionString: String, driver: String)

}
