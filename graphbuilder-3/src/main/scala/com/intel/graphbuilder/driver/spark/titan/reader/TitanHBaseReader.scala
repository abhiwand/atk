package com.intel.graphbuilder.driver.spark.titan.reader

import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.{TitanAutoPartitioner, TitanGraphConnector}
import com.intel.graphbuilder.io.GBTitanHBaseInputFormat
import com.thinkaurelius.titan.core.util.ReflectiveConfigOptionLoader
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.formats.hbase.TitanHBaseInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * TitanHBaseReader constants.
 */
object TitanHBaseReader {
  val TITAN_HADOOP_PREFIX = "titan.hadoop.input.conf."
  val TITAN_STORAGE_NAMESPACE = GraphDatabaseConfiguration.STORAGE_NS.getName
  val TITAN_STORAGE_HOSTNAME = TITAN_STORAGE_NAMESPACE + "." + GraphDatabaseConfiguration.STORAGE_HOSTS.getName
  val TITAN_STORAGE_TABLENAME = TITAN_STORAGE_NAMESPACE + "." + HBaseStoreManager.HBASE_NS.getName + "." + HBaseStoreManager.HBASE_TABLE.getName
  val TITAN_STORAGE_PORT = TITAN_STORAGE_NAMESPACE + "." + GraphDatabaseConfiguration.STORAGE_PORT.getName
}

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a HBase storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector Connector to Titan
 */
class TitanHBaseReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends TitanReader(sparkContext, titanConnector) {

  import com.intel.graphbuilder.driver.spark.titan.reader.TitanHBaseReader._

  require(titanConfig.containsKey(TITAN_STORAGE_HOSTNAME), "could not find key " + TITAN_STORAGE_HOSTNAME)
  require(titanConfig.containsKey(TITAN_STORAGE_TABLENAME), "could not find key " + TITAN_STORAGE_TABLENAME)

  /**
   * Read Titan graph from a HBase storage backend into a Spark RDD of graph elements.
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
   */
  override def read(): RDD[GraphElement] = {
    val hBaseConfig = createHBaseConfiguration()
    val tableName = titanConfig.getString(TITAN_STORAGE_TABLENAME)

    checkTableExists(hBaseConfig, tableName)

    val hBaseRDD = sparkContext.newAPIHadoopRDD(hBaseConfig, classOf[GBTitanHBaseInputFormat],
      classOf[NullWritable],
      classOf[FaunusVertex])

    new TitanHBaseReaderRDD(hBaseRDD, titanConnector)
  }

  /**
   * Create HBase configuration for connecting to HBase table
   */
  private def createHBaseConfiguration(): org.apache.hadoop.conf.Configuration = {
    val hBaseConfig = HBaseConfiguration.create()

    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanHadoopKey = TITAN_HADOOP_PREFIX + titanKey
        hBaseConfig.set(titanHadoopKey, titanConfig.getProperty(titanKey).toString)
    }

    hBaseConfig
  }

  /**
   * Throw an exception if the HBase table does not exist.
   *
   * @param hBaseConfig HBase configuration
   * @param tableName HBase table name
   */
  private def checkTableExists(hBaseConfig: org.apache.hadoop.conf.Configuration, tableName: String) = {
    val admin = new HBaseAdmin(hBaseConfig)
    if (!admin.isTableAvailable(tableName)) {
      admin.close()
      throw new RuntimeException("Table does not exist:" + tableName)
    }
    admin.close()
  }
}
