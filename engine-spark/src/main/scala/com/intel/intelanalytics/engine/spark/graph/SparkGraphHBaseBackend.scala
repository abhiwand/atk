package com.intel.intelanalytics.engine.spark.graph

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.GraphBackendStorage
import com.intel.intelanalytics.engine.plugin.Invocation

/**
 * Implements graph backend storage in HBase for Spark.
 */
class SparkGraphHBaseBackend(hbaseAdminFactory: HBaseAdminFactory)
    extends GraphBackendStorage
    with EventLogging
    with EventLoggingImplicits {

  /**
   * Deletes a graph's underlying table from HBase.
   * @param graphName The user's name for the graph.
   * @param quiet Whether we attempt to delete quietly(if true) or raise raise an error if table doesn't exist(if false).
   */
  override def deleteUnderlyingTable(graphName: String, quiet: Boolean)(implicit invocation: Invocation): Unit = withContext("deleteUnderlyingTable") {

    val hbaseAdmin = hbaseAdminFactory.createHBaseAdmin()

    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    if (hbaseAdmin.tableExists(tableName)) {
      if (hbaseAdmin.isTableEnabled(tableName)) {
        info(s"disabling hbase table: $tableName")
        hbaseAdmin.disableTable(tableName)
      }
      info(s"deleting hbase table: $tableName")
      hbaseAdmin.deleteTable(tableName)
    }
    else {
      info(s"HBase table $tableName requested for deletion does not exist.")
      if (!quiet) {
        throw new IllegalArgumentException(
          s"HBase table $tableName requested for deletion does not exist.")
      }
    }
  }

  override def renameUnderlyingTable(graphName: String, newName: String)(implicit invocation: Invocation): Unit = {
    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    val newTableName: String = GraphBackendName.convertGraphUserNameToBackendName(newName)
    val snapShotName: String = graphName.concat("_SnapShot")

    val hbaseAdmin = hbaseAdminFactory.createHBaseAdmin()
    if (hbaseAdmin.tableExists(tableName)) {
      if (hbaseAdmin.tableExists(newTableName)) {
        hbaseAdmin.disableTable(newTableName)
        hbaseAdmin.deleteTable(newTableName)
      }
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.snapshot(snapShotName, tableName)
      hbaseAdmin.cloneSnapshot(snapShotName, newTableName)
      hbaseAdmin.deleteSnapshot(snapShotName)
      hbaseAdmin.deleteTable(tableName)
    }
  }

}