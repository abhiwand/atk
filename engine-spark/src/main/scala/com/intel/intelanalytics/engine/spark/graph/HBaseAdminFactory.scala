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

import com.intel.intelanalytics.engine.spark.util.KerberosAuthenticator
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration

/**
 * Create HBaseAdmin instances
 *
 * HBaseAdmin should not be re-used forever: you should create, use, throw away - andl then get another one next time
 */
class HBaseAdminFactory {

  /**
   * HBaseAdmin should not be re-used forever: you should create, use, throw away - and then get another one next time
   */
  def createHBaseAdmin(): HBaseAdmin = {
    val config = new Configuration()

    // for some reason HBaseConfiguration wasn't picking up hbase-default.xml automatically, so manually adding here
    config.addResource(getClass.getClassLoader.getResourceAsStream("hbase-default.xml"))
    config.addResource(getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))

    // Skip check for default hbase version which causes intermittent errors "|hbase-default.xml file seems to be for and old version of HBase (null), this version is 0.98.1-cdh5.1.2|"
    // This error shows up despite setting the correct classpath in bin/api-server.sh and packaging the correct cdh hbase jars
    config.setBoolean("hbase.defaults.for.version.skip", true)
    KerberosAuthenticator.loginConfigurationWithKeyTab(config)

    new HBaseAdmin(HBaseConfiguration.addHbaseResources(config))
  }

}
