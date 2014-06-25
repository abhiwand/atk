package com.intel.intelanalytics.engine.hadoop

import com.intel.intelanalytics.shared.EventLogging
import com.typesafe.config.{ Config, ConfigFactory, ConfigObject, ConfigValue }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ LocalFileSystem, Path }
import org.apache.hadoop.hdfs.DistributedFileSystem

import scala.collection.JavaConverters._
import scala.reflect.io.Directory

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
    val hadoopLocation = rootConfig.getString("intel.analytics.engine.hadoop.configuration.path")
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
    val root: String = rootConfig.getString("intel.analytics.engine.fs.root")
    if (root.startsWith("hdfs"))
      hadoopConfig.set("fs.defaultFS", root)

    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    hadoopConfig.setIfUnset("fs.hdfs.impl",
      classOf[DistributedFileSystem].getName)
    hadoopConfig.setIfUnset("fs.file.impl",
      classOf[LocalFileSystem].getName)
    //require(hadoopConfig.getClassByNameOrNull(classOf[LocalFileSystem].getName) != null)

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
