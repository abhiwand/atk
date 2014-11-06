package com.intel.graphbuilder.driver.spark.titan.reader

import java.lang.reflect.Method

import com.intel.graphbuilder.driver.spark.rdd.TitanHBaseReaderRDD
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.{ TitanAutoPartitioner, TitanGraphConnector }
import com.intel.graphbuilder.io.HBaseTableInputFormat
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import org.apache.hadoop.hbase.client.{ HBaseAdmin, Scan }
import org.apache.hadoop.hbase.mapreduce.{ TableInputFormat, TableMapReduceUtil }
import org.apache.hadoop.hbase.{ HBaseConfiguration, HConstants }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
 * TitanHBaseReader constants.
 */
object TitanHBaseReader {
  val TITAN_STORAGE_HOSTNAME = GraphDatabaseConfiguration.STORAGE_NAMESPACE + "." + GraphDatabaseConfiguration.HOSTNAME_KEY
  val TITAN_STORAGE_TABLENAME = GraphDatabaseConfiguration.STORAGE_NAMESPACE + "." + HBaseStoreManager.TABLE_NAME_KEY
  val TITAN_STORAGE_PORT = GraphDatabaseConfiguration.STORAGE_NAMESPACE + "." + GraphDatabaseConfiguration.PORT_KEY
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
    val tableName = hBaseConfig.get(TableInputFormat.INPUT_TABLE)

    println("Initial HBase config")
    println(TableInputFormat.INPUT_TABLE + "=" + hBaseConfig.get(TableInputFormat.INPUT_TABLE))
    println(HBaseTableInputFormat.NUM_REGION_SPLITS + "=" + hBaseConfig.get(HBaseTableInputFormat.NUM_REGION_SPLITS))

    checkTableExists(hBaseConfig, tableName)

    val hBaseRDD = sparkContext.newAPIHadoopRDD(hBaseConfig, classOf[HBaseTableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    new TitanHBaseReaderRDD(hBaseRDD, titanConnector)
  }

  /**
   * Create HBase configuration for connecting to HBase table
   */
  private def createHBaseConfiguration(): org.apache.hadoop.conf.Configuration = {
    val hBaseConfig = HBaseConfiguration.create()

    val hBaseZookeeperQuorum = titanConfig.getString(TITAN_STORAGE_HOSTNAME)
    val tableName = titanConfig.getString(TITAN_STORAGE_TABLENAME)
    val hBaseZookeeperClientPort = titanConfig.getString(TITAN_STORAGE_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT.toString)
    println("Printing Titan properties")
    titanConfig.getKeys.foreach(k => {
      println("Property " + k + "=" + titanConfig.getProperty(k.toString))
    })
    println("Zookeeper: " + hBaseZookeeperQuorum)
    println("tablename: " + tableName)
    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM, hBaseZookeeperQuorum);
    hBaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, hBaseZookeeperClientPort);
    hBaseConfig.set(TableInputFormat.INPUT_TABLE, tableName)

    println("Initial HBase config")
    println(TableInputFormat.INPUT_TABLE + "=" + hBaseConfig.get(TableInputFormat.INPUT_TABLE))
    println(HBaseTableInputFormat.NUM_REGION_SPLITS + "=" + hBaseConfig.get(HBaseTableInputFormat.NUM_REGION_SPLITS))

    // Auto-configure number of input splits
    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    //val hBaseAdmin = new HBaseAdmin(hBaseConfig)
    titanAutoPartitioner.setHBaseInputSplits(sparkContext, hBaseConfig, tableName)
    configureHBaseScanner(hBaseConfig)

    println("Updated HBase config")
    println(TableInputFormat.INPUT_TABLE + "=" + hBaseConfig.get(TableInputFormat.INPUT_TABLE))
    println(HBaseTableInputFormat.NUM_REGION_SPLITS + "=" + hBaseConfig.get(HBaseTableInputFormat.NUM_REGION_SPLITS))
    hBaseConfig
  }

  /**
   * Configure HBase scanner to filter for Titan's edge store column family.
   *
   * TODO:  consider adding support for scanner optimizations in http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html
   * @param hBaseConfig HBase configuration
   */
  private def configureHBaseScanner(hBaseConfig: org.apache.hadoop.conf.Configuration) = {
    val scanner: Scan = new Scan
    val titanColumnFamilyName = com.thinkaurelius.titan.diskstorage.Backend.EDGESTORE_NAME.getBytes()
    scanner.addFamily(titanColumnFamilyName)

    var converter: Method = null
    try {
      converter = classOf[TableMapReduceUtil].getDeclaredMethod("convertScanToString", classOf[Scan])
      converter.setAccessible(true)
      hBaseConfig.set(TableInputFormat.SCAN, converter.invoke(null, scanner).asInstanceOf[String])
    }
    catch {
      case e: Exception => {
        throw new RuntimeException("Unable to create HBase filter for Titan's edge column family", e)
      }
    }
  }

  /**
   * Throw an exception if the HBase table does not exist.
   *
   * @param hBaseConfig HBase configuration
   * @param tableName HBase table name
   */
  private def checkTableExists(hBaseConfig: org.apache.hadoop.conf.Configuration, tableName: String) = {
    val admin = new HBaseAdmin(hBaseConfig)

    println("Checking table exists for  HBase config. Tablename=" + tableName)
    if (admin == null) println("oh! oh!")
    println(TableInputFormat.INPUT_TABLE + "=" + hBaseConfig.get(TableInputFormat.INPUT_TABLE))
    println(HBaseTableInputFormat.NUM_REGION_SPLITS + "=" + hBaseConfig.get(HBaseTableInputFormat.NUM_REGION_SPLITS))

    if (!admin.isTableAvailable(tableName)) {
      throw new RuntimeException("Table does not exist:" + tableName)
    }
  }
}
