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

package com.intel.intelanalytics.engine.spark.util

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil
import com.intel.intelanalytics.component.ClassLoaderAware

import scala.reflect.io.{ File, Directory, Path }

import scala.util.control.NonFatal

/**
 * Static methods for accessing a Kerberos secured hadoop cluster.
 */
object KerberosAuthenticator extends EventLogging with EventLoggingImplicits with ClassLoaderAware {
  // TODO: Allow support for multiple keytabs once namespaces is implemented

  /**
   * Login to Kerberos cluster using a keytab and principal name specified in config files
   */
  def loginWithKeyTab(): Unit = {
    if (SparkEngineConfig.enableKerberos) {
      println("What's in my path?")
      Directory.Current.get.deepFiles.foreach(println)
      //if kerberos is enabled the following configs will have been set.
      val keyTabPrincipal: String = SparkEngineConfig.kerberosPrincipalName.get
      val keyTabFile: String = SparkEngineConfig.kerberosKeyTabPath.get
      info(s"Authenticate with Kerberos\n\tPrincipal: $keyTabPrincipal\n\tKeyTab File: $keyTabFile")
      UserGroupInformation.loginUserFromKeytab(
        keyTabPrincipal,
        keyTabFile)
    }
  }

  /**
   * Login to Kerberos cluster using a keytab and principal name specified in config files
   * using a specific HadoopConfiguration
   * @param configuration HadoopConfiguration
   * @return UserGroupInformation for Kerberos TGT ticket
   */
  def loginConfigurationWithKeyTab(configuration: Configuration): Unit = withMyClassLoader {
    if (SparkEngineConfig.enableKerberos) {
      UserGroupInformation.setConfiguration(configuration)
      KerberosAuthenticator.loginWithKeyTab()
    }
  }

  /**
   * Login to Kerberos using a keytab and principal name specified in config files via kinit command
   */
  def loginWithKeyTabCLI(): Unit = {
    //Note this method logs executes kinit for the user running ATK Rest Server. This user must be able to get a valid TGT.
    if (SparkEngineConfig.enableKerberos) {
      try {
        info("Authenticate to Kerberos using kinit")
        val command = s"kinit ${SparkEngineConfig.kerberosPrincipalName.get} -k -t ${SparkEngineConfig.kerberosKeyTabPath.get}"
        val p = Runtime.getRuntime.exec(command)
        val exitValue = p.waitFor()
        info(s"kinit exited with Exit Value: $exitValue")
        if (exitValue == 1) {
          warn(s"Problem executing kinit. May not have valid TGT.")
        }
      }
      catch {
        case NonFatal(e) => error("Error executing kinit.", exception = e)
      }
    }
  }

}
