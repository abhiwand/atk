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

package com.intel.intelanalytics.algorithm.util

import com.intel.graphbuilder.graph.titan.TitanAutoPartitioner
import com.intel.intelanalytics.domain.graph.GraphEntity
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphHBaseBackend, GraphBackendName, GraphBuilderConfigFactory }
import com.intel.intelanalytics.engine.spark.util.KerberosAuthenticator
import com.typesafe.config.{ ConfigValue, ConfigObject, Config }
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Try

object GiraphConfigurationUtil {

  /**
   * Set a value in the hadoop configuration, if the argument is not None.
   * @param hadoopConfiguration the configuration to update
   * @param hadoopKey the key name to set
   * @param arg the value to use, if it is defined
   */
  def set(hadoopConfiguration: Configuration, hadoopKey: String, arg: Option[Any]) = arg.foreach { value =>
    hadoopConfiguration.set(hadoopKey, value.toString)
  }

  /**
   * Create new Hadoop Configuration object, preloaded with the properties
   * specified in the given Config object under the provided key.
   * @param config the Config object from which to copy properties to the Hadoop Configuration
   * @param key the starting point in the Config object. Defaults to "hadoop".
   * @return a populated Hadoop Configuration object.
   */
  def newHadoopConfigurationFrom(config: Config, key: String = "hadoop"): org.apache.hadoop.conf.Configuration = {
    require(config != null, "Config cannot be null")
    require(key != null, "Key cannot be null")
    val hConf = new Configuration()

    KerberosAuthenticator.loginConfigurationWithKeyTab(hConf)

    val properties = flattenConfig(config.getConfig(key))
    properties.foreach { kv =>
      println(s"Setting ${kv._1} to ${kv._2}")
      hConf.set(kv._1, kv._2)
    }
    hConf
  }

  /**
   * Update the Hadoop configuration object with the Titan configuration
   *
   * @param hConf the Hadoop configuration object to update
   * @param config the Config object from which to copy Titan properties to the Hadoop Configuration
   * @param graph the graph object containing the Titan graph name
   */
  def initializeTitanConfig(hConf: Configuration, config: Config, graph: GraphEntity): Unit = {

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)
    val storageBackend = titanConfig.getString("storage.backend")

    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanGiraphKey = "giraph.titan.input." + titanKey
        set(hConf, titanGiraphKey, Option[Any](titanConfig.getProperty(titanKey)))
    }

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    val numGiraphWorkers = Try(config.getInt("giraph.giraph.maxWorkers")).getOrElse(0)
    titanAutoPartitioner.setGiraphHBaseInputSplits(hConf, numGiraphWorkers)
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

}
