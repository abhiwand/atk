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

import com.intel.event.EventLogging
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.{ SparkConf, SparkContext }

class SparkContextFactoryManager() extends EventLogging {
  //TODO read the strategy from the config file

  def getContext(user: String, description: String): SparkContext = withContext("engine.SparkContextFactoryManager") {

    val jarPath = Boot.getJar("engine-spark")
    val sparkConf = new SparkConf()
      .setMaster(SparkEngineConfig.sparkMaster)
      .setSparkHome(SparkEngineConfig.sparkHome)
      .setAppName(s"intel-analytics:$user:$description")
      .setJars(Seq(jarPath.getPath))

    sparkConf.setAll(SparkEngineConfig.sparkConfProperties)

    info("SparkConf settings: " + sparkConf.toDebugString)

    new SparkContext(sparkConf)
  }

  def context(implicit user: UserPrincipal, description: String): SparkContext = getContext(user.user.apiKey.getOrElse(
    throw new RuntimeException("User didn't have an apiKey which shouldn't be possible if they were authenticated")), description)
}