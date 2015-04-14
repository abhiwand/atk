//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.graph

import java.io.OutputStream

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.domain.graph.GraphEntity
import com.intel.intelanalytics.engine.GraphBackendStorage
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.util.KerberosAuthenticator
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.client.HBaseAdmin
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.{ Success, Failure }

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
    val tableName: String = graphName
    try {
      KerberosAuthenticator.loginWithKeyTabCLI()
      info(s"Trying to copy the HBase Table: $tableName")
      val hbaseAdmin = hbaseAdminFactory.createHBaseAdmin()

      if (hbaseAdmin.tableExists(tableName)) {
        info(s"Copying hbase table: $tableName to $newName")
        hbaseAdmin.snapshot(tableName + "_copysnap", tableName)
        hbaseAdmin.cloneSnapshot(tableName + "_copysnap", newName)
      }
      else {
        error(s"HBase table $tableName requested for copy does not exist.")
        throw new IllegalArgumentException(
          s"HBase table $tableName requested for copy does not exist.")
      }
    }
    catch {
      case ex: Exception => {

        info(s"Unable to copy the requested HBase table: $tableName.", exception = ex)
        throw new Exception(s"Unable to copy the requested HBase table $tableName.", ex)
      }
    }
  }

  /**
   * Deletes a graph's underlying table from HBase.
   * @param graphName The user's name for the graph.
   * @param quiet Whether we attempt to delete quietly(if true) or raise raise an error if table doesn't exist(if false).
   * @param inBackground true to let the actual deletion happen sometime in the future, false to block until delete is complete
   *                           true will also prevent exceptions from bubbling up
   */
  override def deleteUnderlyingTable(graphName: String, quiet: Boolean, inBackground: Boolean)(implicit invocation: Invocation): Unit = withContext("deleteUnderlyingTable") {
    if (inBackground) {
      Future {
        performDelete(graphName, quiet)
      } onComplete {
        case Success(ok) => info(s"deleting table '$graphName' completed, quiet: $quiet, inBackground: $inBackground")
        case Failure(ex) => error(s"deleting table '$graphName' failed, quiet: $quiet, inBackground: $inBackground", exception = ex)
      }
      Unit
    }
    else {
      performDelete(graphName, quiet)
    }
  }

  private def performDelete(graphName: String, quiet: Boolean): Unit = {
    val tableName: String = graphName
    try {
      KerberosAuthenticator.loginWithKeyTabCLI()
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
        info(s"HBase table $tableName requested for deletion does not exist.")
        if (!quiet) {
          throw new IllegalArgumentException(
            s"HBase table $tableName requested for deletion does not exist.")
        }
      }
    }
    catch {
      case e: Throwable => {
        error(s"Unable to delete the requested HBase table: $tableName.", exception = e)
        if (!quiet) {
          throw new IllegalArgumentException(
            s"Unable to delete the requested HBase table: $tableName.")
        }
      }
    }
  }
}
