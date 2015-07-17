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

package com.intel.taproot.analytics.engine.spark.hadoop

import com.typesafe.config.{ Config, ConfigFactory, ConfigObject, ConfigValue }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ LocalFileSystem, Path }
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.reflect.io.Directory
import com.intel.taproot.event.EventLogging

/**
 * Helper methods for classes that need to use Hadoop classes
 */
trait HadoopSupport extends EventLogging {

  /**
   * Create a new Hadoop Configuration object
   * @return a Hadoop Configuration
   */
  def newHadoopConfiguration(): Configuration = {
    val hadoopConfig = new Configuration()

    //TODO: refactor so we don't need root config here.
    val rootConfig = ConfigFactory.load
    val hadoopLocation = rootConfig.getString("intel.taproot.analytics.engine.hadoop.configuration.path")
    Directory(hadoopLocation).files match {
      case Seq() => warn(s"No Hadoop configuration files were found at $hadoopLocation.")
      case fs =>
        fs.filter {
          _.extension == "xml"
        }.foreach {
          f =>
            val name = f.toString()
            println(s"Adding resource: $name")
            hadoopConfig.addResource(new Path(name))
        }
    }
    val root: String = rootConfig.getString("intel.taproot.analytics.engine.fs.root")
    if (root.startsWith("hdfs"))
      hadoopConfig.set("fs.defaultFS", root)

    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    hadoopConfig.setIfUnset("fs.hdfs.impl",
      classOf[DistributedFileSystem].getName)
    hadoopConfig.setIfUnset("fs.file.impl",
      classOf[LocalFileSystem].getName)

    hadoopConfig
  }

  /**
   * Create new Hadoop Configuration object, preloaded with the properties
   * specified in the given Config object under the provided key.
   * @param config the Config object from which to copy properties to the Hadoop Configuration
   * @param key the starting point in the Config object. Defaults to "hadoop".
   * @return a populated Hadoop Configuration object.
   */
  def newHadoopConfigurationFrom(config: Config, key: String = "hadoop") = {
    require(config != null, "Config cannot be null")
    require(key != null, "Key cannot be null")
    val hConf = newHadoopConfiguration()
    val properties = flattenConfig(config.getConfig(key))
    properties.foreach { kv =>
      println(s"Setting ${kv._1} to ${kv._2}")
      hConf.set(kv._1, kv._2)
    }
    hConf
  }

  /**
   * Flatten a nested Config structure down to a simple dictionary that maps complex keys to
   * a string value, similar to java.util.Properties.
   *
   * @param config the config to flatten
   * @return a map of property names to values
   */
  private def flattenConfig(config: Config, prefix: String = ""): Map[String, String] = {
    val result = config.root.asScala.foldLeft(Map.empty[String, String]) {
      (map, kv) =>
        kv._2 match {
          case co: ConfigObject =>
            val nested = flattenConfig(co.toConfig, prefix = prefix + kv._1 + ".")
            map ++ nested
          case value: ConfigValue =>
            map + (prefix + kv._1 -> value.unwrapped().toString)
        }
    }
    result
  }

  /**
   * Set a value in the hadoop configuration, if the argument is not None.
   * @param hadoopConfiguration the configuration to update
   * @param hadoopKey the key name to set
   * @param arg the value to use, if it is defined
   */
  def set(hadoopConfiguration: Configuration, hadoopKey: String, arg: Option[Any]) = arg.foreach { value =>
    hadoopConfiguration.set(hadoopKey, value.toString)
  }
}

object HadoopSupport extends EventLogging {
  def killYarnJob(jobName: String): Unit = {
    val yarnClient = YarnClient.createYarnClient
    val yarnConf = new YarnConfiguration(new Configuration())
    yarnClient.init(yarnConf)
    yarnClient.start()
    val app = yarnClient.getApplications.find(ap => ap.getName == jobName)
    if (app.isDefined) {
      info(s"Killing yarn application ${app.get.getApplicationId} which corresponds to command $jobName")
      yarnClient.killApplication(app.get.getApplicationId)
    }
    else {
      throw new Exception(s"Could not cancel command $jobName as application could not be found on yarn")
    }
    yarnClient.stop()
  }
}
