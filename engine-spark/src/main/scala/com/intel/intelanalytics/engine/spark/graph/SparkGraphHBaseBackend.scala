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
   * @param quiet Whether we attempt to delete quietly(if true) or raise raise an error if table doesn't exist(if false).
   */
  override def deleteUnderlyingTable(graphName: String, quiet: Boolean): Unit = {

    val tableName: String = GraphName.convertGraphUserNameToBackendName(graphName)
    if (hbaseAdmin.tableExists(tableName)) {
      if (hbaseAdmin.isTableEnabled(tableName)) {
        hbaseAdmin.disableTable(tableName)
      }
      hbaseAdmin.deleteTable(tableName)
    }
    else {
      if (!quiet) {
        throw new IllegalArgumentException(
          "SparkGraphHBaseBackend.deleteTable:  HBase table " + tableName + " requested for deletion does not exist.")
      }
    }
  }

  override def renameUnderlyingTable(graphName: String, newName: String): Unit = {
    val tableName: String = GraphName.convertGraphUserNameToBackendName(graphName)
    val newTableName: String = GraphName.convertGraphUserNameToBackendName(newName)
    val snapShotName: String = "snapShot"

    if (hbaseAdmin.tableExists(tableName)) {

      if (hbaseAdmin.isTableEnabled(tableName))
        { hbaseAdmin.disableTable(tableName)
        }

      hbaseAdmin.snapshot(snapShotName, tableName)
      hbaseAdmin.cloneSnapshot(snapShotName, newTableName)
      hbaseAdmin.deleteSnapshot(snapShotName)
      hbaseAdmin.deleteTable(tableName)
      hbaseAdmin.enableTable(newTableName)
    }
  }
}