package com.intel.graphbuilder.driver.spark.titan.reader

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import org.apache.spark.SparkContext
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration

/**
 * TitanReader constants.
 */
object TitanReader {
  val TITAN_STORAGE_BACKEND = GraphDatabaseConfiguration.STORAGE_NAMESPACE + "." + GraphDatabaseConfiguration.STORAGE_BACKEND_KEY
  val TITAN_READER_GB_ID = "titanPhysicalId" //TODO: Replace with a user-defined label
}

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector connector to Titan
 */
case class TitanReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends Serializable {

  import TitanReader._

  val titanConfig = titanConnector.config

  /**
   * Read Titan graph from storage backend into a Spark RDD of graph elements,
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
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
