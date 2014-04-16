package com.intel.graphbuilder.driver.spark.titan.examples

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.{GraphUtils, SerializableBaseConfiguration}
import java.util.Date

/**
 * Utility for use during development.
 */
object TitanQueryDriver {

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "cassandra")
  titanConfig.setProperty("storage.hostname", "127.0.0.1")
  titanConfig.setProperty("storage.keyspace", "netflix")

  def main(args: Array[String]) = {

    val titanConnector = new TitanGraphConnector(titanConfig)

    val graph = titanConnector.connect()
    try {
      println(GraphUtils.dumpGraph(graph))
    }
    finally {
      graph.shutdown()
    }

    println("done " + new Date())

  }
}
