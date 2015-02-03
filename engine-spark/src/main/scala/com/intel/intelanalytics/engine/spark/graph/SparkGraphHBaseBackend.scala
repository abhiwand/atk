package com.intel.intelanalytics.engine.spark.graph

import java.io.OutputStream

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
    // TODO: To be deleted later. Workaround for TRIB: 4318.
    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    var outputStream: OutputStream = null
    try {
      //create a new process
      val p = Runtime.getRuntime.exec("hbase shell -n")
      outputStream = p.getOutputStream

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
    finally
    {
      outputStream.flush()
      outputStream.close()
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