package com.intel.graphbuilder.driver.spark.titan.reader

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import org.apache.spark.SparkContext
import TitanReaderConstants.TITAN_STORAGE_BACKEND

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a storage backend..
 *
 * @param sparkContext Spark context
 * @param titanConnector connector to Titan
 */
case class TitanReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends Serializable {

  val titanConfig = titanConnector.config

  /**
   * Read Titan graph from storage backend into a Spark RDD of graph elements
   *
   * @return RDD of GraphBuilder vertices and edges
   */
  def read(): RDD[GraphElement] = {
    val storageBackend = titanConfig.getString(TITAN_STORAGE_BACKEND)

    val titanReaderRDD = storageBackend match {
      case "hbase" => {
        val titanHBaseReader = new TitanHBaseReader(sparkContext, titanConnector)
        titanHBaseReader.read()
      }
      case _ => throw new RuntimeException {
        "Unsupported storage backend for Titan reader: " + storageBackend
      }
    }

    titanReaderRDD.distinct()
  }
}
