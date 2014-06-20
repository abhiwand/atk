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

package com.intel.intelanalytics.engine.spark.context

import com.typesafe.config.Config
import org.apache.spark.{ SparkConf, SparkContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.engine.spark.SparkEngineConfig

/**
 * Had to extract SparkContext creation logic from the SparkContextManagementStrategy for better testability
 */
class SparkContextFactory extends EventLogging {

  def createSparkContext(configuration: Config, appName: String): SparkContext = withContext("engine.sparkContextFactory") {
    val sparkHome = configuration.getString("intel.analytics.spark.home")
    val sparkMaster = configuration.getString("intel.analytics.spark.master")

    val jarPath = Boot.getJar("engine-spark")
    val sparkConf = new SparkConf()
      .setMaster(sparkMaster)
      .setSparkHome(sparkHome)
      .setAppName(appName)
      .setJars(Seq(jarPath.getPath))

    sparkConf.setAll(SparkEngineConfig.sparkConfProperties)

    info("SparkConf settings: " + sparkConf.toDebugString)

    new SparkContext(sparkConf)
  }
}
