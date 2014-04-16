package com.intel.graphbuilder.graph.titan

import com.intel.graphbuilder.graph.GraphConnector
import com.thinkaurelius.titan.core.{TitanGraph, TitanFactory}
import java.io.File
import org.apache.commons.configuration.{PropertiesConfiguration, Configuration}

/**
 * Get a connection to Titan.
 * <p>
 * This was needed because a 'Connector' can be serialized across the network, where an live connection can't.
 * </p>
 */
class TitanGraphConnector(config: Configuration) extends GraphConnector with Serializable {

  /**
   * Initialize using a properties configuration file.
   * @param propertiesFile a .properties file, see example-titan.properties and Titan docs
   */
  def this(propertiesFile: File) {
    this(new PropertiesConfiguration(propertiesFile))
  }

  /**
   * Get a connection to a graph database
   */
  override def connect(): TitanGraph = {
    TitanFactory.open(config)
  }

}
