/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
