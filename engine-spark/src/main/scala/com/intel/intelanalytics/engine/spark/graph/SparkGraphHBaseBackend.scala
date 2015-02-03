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
   * makes a copy of the titan graph's underlying table in the HBase
   *
   * @param graphName Name of the titan graph that is to copied
   * @param newName Name provided for the copy
   * @return
   */
  override def copyUnderlyingTable(graphName: String, newName: Option[String])(implicit invocation: Invocation): Unit = {
    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    try {
      info(s"Trying to copy the HBase Table: $tableName")
      val p = Runtime.getRuntime.exec("hbase shell -n")
      val outputStream = p.getOutputStream

      val givenName = GraphBackendName.convertGraphUserNameToBackendName(newName.getOrElse(tableName + "-2"))
      IOUtils.write(s"snapshot '${tableName}', '${tableName}-snapshot'\nclone_snapshot '${tableName}-snapshot', '${givenName}'\n", outputStream)
      outputStream.flush()
      outputStream.close()

      IOUtils.readLines(p.getInputStream).map(infoMsg => info(infoMsg))
      IOUtils.readLines(p.getErrorStream).map(errorMsg => warn(errorMsg))

      val exitValue = p.waitFor()
      info(s"Hbase shell exited with Exit Value: $exitValue")

      if (exitValue != 0) {
        throw new IllegalArgumentException(
          s"Unable to copy the requested HBase table $tableName.")
      }
    }
  }

  /**
   * Deletes a graph's underlying table from HBase.
   * @param graphName The user's name for the graph.
   * @param quiet Whether we attempt to delete quietly(if true) or raise raise an error if table doesn't exist(if false).
   */
  override def deleteUnderlyingTable(graphName: String, quiet: Boolean)(implicit invocation: Invocation): Unit = withContext("deleteUnderlyingTable") {
    // TODO: To be deleted later. Workaround for TRIB: 4318.
    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    try {
      //create a new process
      val p = Runtime.getRuntime.exec("hbase shell -n")
      val outputStream = p.getOutputStream

      IOUtils.write("disable tableName\nmajor_compact \".META.\"\ndrop tableName", outputStream)
      outputStream.flush()
      outputStream.close()

      IOUtils.readLines(p.getInputStream).map(infoMsg => info(infoMsg))
      IOUtils.readLines(p.getErrorStream).map(errorMsg => error(errorMsg))

      val exitValue = p.waitFor()
      if (exitValue != 0) {
        info(s"Hbase shell exited with Exit Value: $exitValue")
      }
    }
    catch {
      case e: Exception =>
        info(s"Unable to delete the requested HBase table HBase table $tableName. Exception: $e")
        if (!quiet) {
          throw new IllegalArgumentException(
            s"Unable to delete the requested HBase table $tableName. Exception: $e")
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