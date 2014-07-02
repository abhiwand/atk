//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.repository.util

import java.sql.DriverManager

/**
 * Developer utility for testing JDBC Connectivity
 */
object JdbcConnectionPing {

  // URL Format: "jdbc:postgresql://host:port/database"
  var url = "jdbc:postgresql://localhost:5432/metastore"
  var user = "metastore"
  var password = "Tribeca123"

  /**
   * Establish JDBC Connection, read now() from database
   * @param args url, user, password
   */
  def main(args: Array[String]) {

    if (args.length == 3) {
      url = args(1)
      user = args(2)
      password = args(3)
    }

    println("Trying to Connect (url: " + url + ", user: " + user + ", password: " + password.replaceAll(".", "*") + ")")

    val connection = DriverManager.getConnection(url, user, password)
    val resultSet = connection.createStatement().executeQuery("SELECT now()")
    while (resultSet.next()) {
      println("SELECT now() = " + resultSet.getString(1))
      println("Test was SUCCESSFUL")
    }

  }
}
