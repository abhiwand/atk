package com.intel.intelanalytics.repository

import scala.slick.driver.H2Driver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FlatSpec}
import scala.util.Random

/**
 * Trait can be mixed into ScalaTests to provide a meta store backed by H2.
 *
 * Creates and drops all tables before and after each test.
 */
trait SlickMetaStoreH2Testing extends FlatSpec with BeforeAndAfter {

  lazy val slickMetaStoreComponent: SlickMetaStoreComponent = new SlickMetaStoreComponent with DbProfileComponent {
    override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:iatest" + Random.nextInt() + ";DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
  }

  before {
    // initialize tables
    slickMetaStoreComponent.metaStore.createAllTables()
  }

  after {
    // drop all tables
    slickMetaStoreComponent.metaStore.dropAllTables()
  }

}
