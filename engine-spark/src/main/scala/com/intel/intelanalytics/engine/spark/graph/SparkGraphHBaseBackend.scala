package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import com.intel.intelanalytics.engine.GraphBackendStorage

/**
 * Implements graph backend storage in HBase for Spark.
 */
class SparkGraphHBaseBackend(hbaseAdmin: => HBaseAdmin) extends GraphBackendStorage with EventLogging {

  /**
   * Deletes a graph's underlying table from HBase.
   * @param graphName The user's name for the graph.
   */
  override def deleteUnderlyingTable(graphName: String): Unit = {

    val tableName: String = GraphName.convertGraphUserNameToBackendName(graphName)

    if (hbaseAdmin.tableExists(tableName)) {
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.deleteTable(tableName)
    }
    else {
      throw new IllegalArgumentException(
        "SparkGraphHBaseBackend.deleteTable:  HBase table " + tableName + " requested for deletion does not exist.")
    }
  }

  override def renameUnderlyingTable(graphName: String, newName: String): Unit = {
    val tableName: String = GraphName.convertGraphUserNameToBackendName(graphName)
    val newTableName: String = GraphName.convertGraphUserNameToBackendName(newName)
    val snapShotName: String = "someName"

    if (hbaseAdmin.tableExists(tableName)) {
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.snapshot(snapShotName, tableName)
      hbaseAdmin.cloneSnapshot(snapShotName, newTableName)
      hbaseAdmin.deleteSnapshot(snapShotName)
      hbaseAdmin.deleteTable(tableName)
    }
  }
}