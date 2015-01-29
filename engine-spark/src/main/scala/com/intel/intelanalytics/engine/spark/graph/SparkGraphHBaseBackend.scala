package com.intel.intelanalytics.engine.spark.graph

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.GraphBackendStorage
import com.intel.intelanalytics.engine.plugin.Invocation
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._

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
    // TODO: To be evaluated later per TRIB-4413. Workaround for TRIB: 4318.
    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    try {
      info(s"Trying to delete the HBase Table: $tableName using HBaseAdmin.")
      val hbaseAdmin = hbaseAdminFactory.createHBaseAdmin()

      if (hbaseAdmin.tableExists(tableName)) {
        if (hbaseAdmin.isTableEnabled(tableName)) {
          info(s"disabling hbase table: $tableName")
          hbaseAdmin.disableTable(tableName)
        }
        info(s"deleting hbase table: $tableName")
        hbaseAdmin.deleteTable(tableName)
      }
      else {
        info(s"The HBase Table: $tableName does not exist in the HBase.")
      }
    }
    catch {
      case _ => {
        info(s"Unable to delete HBase Table: $tableName using the HBaseAdmin. Trying to delete it using HBase shell.")
        //create a new process
        val p = Runtime.getRuntime.exec("hbase shell -n")
        val outputStream = p.getOutputStream

        IOUtils.write(s"disable '${tableName}'\ndrop '${tableName}'\n", outputStream)
        outputStream.flush()
        outputStream.close()

        IOUtils.readLines(p.getInputStream).map(infoMsg => info(infoMsg))
        IOUtils.readLines(p.getErrorStream).map(errorMsg => warn(errorMsg))

        val exitValue = p.waitFor()
        info(s"Hbase shell exited with Exit Value: $exitValue")

        if (!quiet && exitValue != 0) {
          throw new IllegalArgumentException(
            s"Unable to delete the requested HBase table $tableName.")
        }
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