package com.intel.intelanalytics.repository

import org.scalatest.{ Matchers, FlatSpec }
import scala.slick.driver.{ PostgresDriver, H2Driver }

class ProfileSpec extends FlatSpec with Matchers {

  "Profile" should "be able to determine the JdbcProfile from the driver name org.h2.Driver" in {
    assert(Profile.jdbcProfileForDriver("org.h2.Driver").isInstanceOf[H2Driver])
  }

  it should "be able to determine the JdbcProfile from the driver name org.postgresql.Driver" in {
    assert(Profile.jdbcProfileForDriver("org.postgresql.Driver").isInstanceOf[PostgresDriver])
  }

  it should "throw an exception for unsupported drivers" in {
    try {
      Profile.jdbcProfileForDriver("com.example.Driver")

      fail("The expected Exception was NOT thrown")
    }
    catch {
      case e: IllegalArgumentException => // Expected
    }
  }
}
