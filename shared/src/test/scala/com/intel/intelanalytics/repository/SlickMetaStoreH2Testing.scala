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

import org.scalatest.{ BeforeAndAfter, FlatSpec }

import scala.slick.driver.H2Driver
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
    slickMetaStoreComponent.metaStore.initializeSchema()
  }

  after {
    // drop all tables
    slickMetaStoreComponent.metaStore.dropAllTables()
  }

}
