package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * TitanReader constants.
 */
object TitanReader {
  val TITAN_STORAGE_NS = GraphDatabaseConfiguration.STORAGE_NS.getName
  val TITAN_STORAGE_BACKEND = TITAN_STORAGE_NS + "." + GraphDatabaseConfiguration.STORAGE_BACKEND.getName
  val TITAN_READER_DEFAULT_GB_ID = "titanPhysicalId" //TODO: Replace with a user-defined label
}

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector connector to Titan
 */
case class TitanReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends Serializable {

  import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader._

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

    titanReaderRDD
  }
}
