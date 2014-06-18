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

package com.intel.intelanalytics.shared

import com.typesafe.config.ConfigFactory

/**
 * Configuration that is shared between both the API Server and the Engine.
 *
 * This is our wrapper for Typesafe config.
 *
 * See ApiServiceConfig and SparkEngineConfig.
 */
trait SharedConfig {

  val config = ConfigFactory.load().withFallback(ConfigFactory.load("engine.conf"))

  // val's are not lazy because failing early is better
  val metaStoreConnectionUrl: String = nonEmptyString("intel.analytics.metastore.connection.url")
  val metaStoreConnectionDriver: String = nonEmptyString("intel.analytics.metastore.connection.driver")
  val metaStoreConnectionUsername: String = config.getString("intel.analytics.metastore.connection.username")
  val metaStoreConnectionPassword: String = config.getString("intel.analytics.metastore.connection.password")

  /**
   * Get a String but throw Exception if it is empty
   */
  protected def nonEmptyString(key: String): String = {
    config.getString(key) match {
      case "" => throw new IllegalArgumentException(key + " cannot be empty!")
      case s: String => s
    }
  }

}
