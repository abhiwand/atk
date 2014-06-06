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

package com.intel.intelanalytics.engine.spark

import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._

class SparkEngineConfiguration(conf: => Config = ConfigFactory.load()) {
  lazy val config = conf
  lazy val sparkHome = conf.getString("intel.analytics.spark.home")
  lazy val sparkMaster = conf.getString("intel.analytics.spark.master")
  lazy val defaultTimeout = conf.getInt("intel.analytics.engine.defaultTimeout").seconds
  lazy val connectionString = conf.getString("intel.analytics.metastore.connection.url") match {
    case "" | null => throw new Exception("No metastore connection url specified in configuration")
    case u => u
  }
  lazy val driver = conf.getString("intel.analytics.metastore.connection.driver") match {
    case "" | null => throw new Exception("No metastore driver specified in configuration")
    case d => d
  }

  lazy val fsRoot = conf.getString("intel.analytics.fs.root")
}
