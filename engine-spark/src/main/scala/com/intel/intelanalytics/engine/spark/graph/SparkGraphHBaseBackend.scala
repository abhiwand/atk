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
   * makes a copy of the titan graph's underlying table in the HBase
   *
   * @param graphName Name of the titan graph that is to copied
   * @param newName Name provided for the copy
   * @return
   */
  override def copyUnderlyingTable(graphName: String, newName: String)(implicit invocation: Invocation): Unit = {
    //TODO: switch to HBaseAdmin instead of shelling out (doing it this way for now because of classloading bugs with HBaseAdmin) TRIB-4318
    val tableName: String = GraphBackendName.convertGraphUserNameToBackendName(graphName)
    var outputStream: OutputStream = null
    try {
      info(s"Trying to copy the HBase Table: $tableName")
      val p = Runtime.getRuntime.exec("hbase shell -n")
      outputStream = p.getOutputStream

      IOUtils.write(s"snapshot '${tableName}', '${tableName}-snapshot'\nclone_snapshot '${tableName}-snapshot', '${newName}'\ndelete_snapshot '${tableName}-snapshot'\n", outputStream)
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
    catch {
      case _ => {
        info(s"Unable to copy the requested HBase table: $tableName.")
        throw new IllegalArgumentException(
          s"Unable to copy the requested HBase table $tableName.")
      }
    }
    finally {
      outputStream.flush()
      outputStream.close()
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
    var outputStream: OutputStream = null
    try {
      //create a new process
      val p = Runtime.getRuntime.exec("hbase shell -n")
      outputStream = p.getOutputStream

      IOUtils.write(s"disable '${tableName}'\ndrop '${tableName}'\n", outputStream)
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
      case _ => {
        info(s"Unable to delete the requested HBase table: $tableName.")
        if (!quiet) {
          throw new IllegalArgumentException(
            s"Unable to delete the requested HBase table $tableName.")
        }
      }
    }
    finally {
      outputStream.flush()
      outputStream.close()
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