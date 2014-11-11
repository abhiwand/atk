package com.intel.graphbuilder.driver.spark.titan.reader

import java.nio.ByteBuffer

import com.intel.graphbuilder.driver.spark.rdd.TitanReaderRDD
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader._
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.diskstorage.Backend
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraInputFormat
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{ SliceRange, SlicePredicate }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a Cassandra storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector Connector to Titan
 */
class TitanCassandraReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends TitanReader(sparkContext, titanConnector) {
  require(titanConfig.containsKey(TITAN_STORAGE_HOSTNAME), "could not find key " + TITAN_STORAGE_HOSTNAME)
  require(titanConfig.containsKey(TITAN_STORAGE_CASSANDRA_KEYSPACE), "could not find key " + TITAN_STORAGE_CASSANDRA_KEYSPACE)

  /**
   * Read Titan graph from a Cassandra storage backend into a Spark RDD of graph elements.
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
   */
  override def read(): RDD[GraphElement] = {
    val cassandraConfig = createCassandraConfiguration()

    val cassandraRDD = sparkContext.newAPIHadoopRDD(cassandraConfig, classOf[TitanCassandraInputFormat],
      classOf[NullWritable],
      classOf[FaunusVertex])

    new TitanReaderRDD(cassandraRDD, titanConnector)
  }

  /**
   * Create Cassandra configuration for connecting to Cassandra table
   */
  private def createCassandraConfiguration(): org.apache.hadoop.conf.Configuration = {
    val cassandraConfig = sparkContext.hadoopConfiguration //new org.apache.hadoop.conf.Configuration()
    val tableName = titanConfig.getString(TITAN_STORAGE_CASSANDRA_KEYSPACE)
    val port = titanConfig.getString(TITAN_STORAGE_PORT)
    val hostnames = titanConfig.getString(TITAN_STORAGE_HOSTNAME)
    val wideRows = titanConfig.getBoolean(TITAN_CASSANDRA_INPUT_WIDEROWS, false)
    val predicate: SlicePredicate = getSlicePredicate

    ConfigHelper.setInputSlicePredicate(cassandraConfig, predicate)
    ConfigHelper.setInputPartitioner(cassandraConfig, "Murmur3Partitioner")
    ConfigHelper.setOutputPartitioner(cassandraConfig, "Murmur3Partitioner")
    ConfigHelper.setInputRpcPort(cassandraConfig, port)
    ConfigHelper.setInputColumnFamily(cassandraConfig, tableName, Backend.EDGESTORE_NAME, wideRows)

    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanHadoopKey = TITAN_HADOOP_PREFIX + titanKey
        cassandraConfig.set(titanHadoopKey, titanConfig.getProperty(titanKey).toString)
    }

    cassandraConfig
  }

  /**
   * Get Range of rows (Slice predicate) for Cassandra input format
   * @return Slice predicate
   */
  private def getSlicePredicate: SlicePredicate = {
    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
    predicate
  }
}
