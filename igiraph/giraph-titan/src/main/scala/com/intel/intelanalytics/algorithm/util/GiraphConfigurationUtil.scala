package com.intel.intelanalytics.algorithm.util

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.typesafe.config.{ ConfigValue, ConfigObject, Config }
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._

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
  def newHadoopConfigurationFrom(config: Config, key: String = "hadoop") = {
    require(config != null, "Config cannot be null")
    require(key != null, "Key cannot be null")
    val hConf = new Configuration()
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
  def flattenConfig(config: Config, prefix: String = ""): Map[String, String] = {
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

  def initializeTitanConfig(hConf: Configuration, titanConf: Map[String, String], graph: Graph) = {
    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    val titanStorageBackend = titanConf.get("titan.load.storage.backend").getOrElse("")
    val titanTableNameKey = TitanGraphConnector.getTitanTableNameKey(titanStorageBackend)
    set(hConf, "giraph.titan.input.storage.backend", titanConf.get("titan.load.storage.backend"))
    set(hConf, "giraph.titan.input." + titanTableNameKey, titanConf.get("titan.load.storage.hostname"))
    set(hConf, "giraph.titan.input.storage.tablename", Option[Any](iatGraphName))
    set(hConf, "giraph.titan.input.storage.port", titanConf.get("titan.load.storage.port"))
  }

}