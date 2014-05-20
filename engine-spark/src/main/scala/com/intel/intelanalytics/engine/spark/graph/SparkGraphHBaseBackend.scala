package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import com.intel.intelanalytics.engine.{GraphBackendStorage, GraphStorage}

/**
 * This is kind of hacky, but no more so than Titan is kind of hacky.
 */
class SparkGraphHBaseBackend extends GraphBackendStorage with EventLogging {

  val conf = new HBaseConfiguration()
  val hbaseAdmin = new HBaseAdmin(conf)

  val iatGraphTablePrefix : String = "titan_graph_"

  override def deleteTable(name : String) : Unit = {

    val tableName : String = iatGraphTablePrefix + name

    if (hbaseAdmin.tableExists(tableName)) {
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.deleteTable(tableName)
    } else {
      throw new IllegalArgumentException(
        "SparkGraphHBaseBackend.deleteTable:  HBase table " + tableName + " requested for deletion does not exist.")
    }

    Unit
  }

  override def listTables(): Seq[String] =  {


    val graphTableNames = hbaseAdmin.listTables().map(x => x.toString()).filter(x => x.startsWith(iatGraphTablePrefix))
    graphTableNames.map(x => x.stripPrefix(iatGraphTablePrefix) )
  }
}
