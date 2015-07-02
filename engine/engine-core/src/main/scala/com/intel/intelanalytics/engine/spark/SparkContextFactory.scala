/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.component.Archive
import com.intel.intelanalytics.engine.plugin.Invocation
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
    if (EngineConfig.reuseSparkContext) {
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
      .setMaster(EngineConfig.sparkMaster)
      .setSparkHome(EngineConfig.sparkHome)
      .setAppName(s"intel-analytics:$userName:$description")

    EngineConfig.sparkConfProperties.foreach { case (k, v) => debug(s"$k->$v") }
    sparkConf.setAll(EngineConfig.sparkConfProperties)

    if (!EngineConfig.disableKryo && kryoRegistrator.isDefined) {
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.set("spark.kryo.registrator", kryoRegistrator.get)
    }

    KerberosAuthenticator.loginWithKeyTab()

    info("SparkConf settings: " + sparkConf.toDebugString)

    val sparkContext = new SparkContext(sparkConf)
    if (!EngineConfig.isSparkOnYarn) {
      // TODO: plugin jars should be added based on the jar the plugin is coming from instead of all of them like this
      val paths = List(jarPath("engine-core"), jarPath("frame-plugins"), jarPath("graph-plugins"), jarPath("model-plugins"))
      info(s"addJar() paths=$paths")
      paths.foreach(sparkContext.addJar)
    }

    sparkContext
  }

  /**
   * Path for jars adding local: prefix or not depending on configuration for use in SparkContext
   *
   * "local:/some/path" means the jar is installed on every worker node.
   *
   * @param archive e.g. "engine-core"
   * @return "local:/usr/lib/intelanalytics/lib/engine-core.jar" or similar
   */
  def jarPath(archive: String): String = {
    if (EngineConfig.sparkAppJarsLocal) {
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
