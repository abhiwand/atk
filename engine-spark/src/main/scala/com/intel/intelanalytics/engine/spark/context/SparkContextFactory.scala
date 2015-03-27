//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.component.Archive
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.util.KerberosAuthenticator
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Class Factory for creating spark contexts
 */
trait SparkContextFactory extends EventLogging with EventLoggingImplicits {

  /**
   * Creates a new sparkContext with the specified kryo classes
   */
  def getContext(description: String, kryoRegistrator: Option[String] = None)(implicit invocation: Invocation): SparkContext = withContext("engine.SparkContextFactory") {
    if (SparkEngineConfig.reuseSparkContext) {
      SparkContextFactory.sharedSparkContext()
    }
    else {
      createContext(description, kryoRegistrator)
    }
  }

  /**
   * Creates a new sparkContext
   */
  def context(description: String, kryoRegistrator: Option[String] = None)(implicit invocation: Invocation): SparkContext =
    getContext(description, kryoRegistrator)

  private def createContext(description: String, kryoRegistrator: Option[String] = None)(implicit invocation: Invocation): SparkContext = {
    val userName = user.user.apiKey.getOrElse(
      throw new RuntimeException("User didn't have an apiKey which shouldn't be possible if they were authenticated"))
    val sparkConf = new SparkConf()
      .setMaster(SparkEngineConfig.sparkMaster)
      .setSparkHome(SparkEngineConfig.sparkHome)
      .setAppName(s"intel-analytics:$userName:$description")

    SparkEngineConfig.sparkConfProperties.foreach { case (k, v) => println(s"$k->$v") }
    sparkConf.setAll(SparkEngineConfig.sparkConfProperties)

    if (!SparkEngineConfig.disableKryo && kryoRegistrator.isDefined) {
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.set("spark.kryo.registrator", kryoRegistrator.get)
    }

    KerberosAuthenticator.loginWithKeyTab()

    info("SparkConf settings: " + sparkConf.toDebugString)

    val sparkContext = new SparkContext(sparkConf)
    if (SparkEngineConfig.sparkMaster != "yarn-cluster")
      sparkContext.addJar(jarPath("engine-spark"))
    sparkContext
  }

  /**
   * Path for jars adding local: prefix or not depending on configuration for use in SparkContext
   *
   * "local:/some/path" means the jar is installed on every worker node.
   *
   * @param archive e.g. "graphon"
   * @return "local:/usr/lib/intelanalytics/lib/graphon.jar" or similar
   */
  def jarPath(archive: String): String = {
    if (SparkEngineConfig.sparkAppJarsLocal) {
      "local:" + StringUtils.removeStart(Archive.getJar(archive).getPath, "file:")
    }
    else {
      Archive.getJar(archive).toString
    }
  }

}

object SparkContextFactory extends SparkContextFactory {

  // for integration tests only
  private var sc: SparkContext = null

  /**
   * This shared SparkContext is for integration tests and regression tests only
   * NOTE: this should break the progress bar.
   */
  private def sharedSparkContext()(implicit invocation: Invocation): SparkContext = {
    this.synchronized {
      if (sc == null) {
        sc = createContext("reused-spark-context", None)
      }
    }
    sc
  }
}
