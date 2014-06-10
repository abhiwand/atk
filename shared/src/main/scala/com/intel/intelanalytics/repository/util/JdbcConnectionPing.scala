package com.intel.intelanalytics.repository.util

import java.sql.DriverManager

/**
 * Developer utility for testing JDBC Connectivity
 */
object JdbcConnectionPing {

  // URL Format: "jdbc:postgresql://host:port/database"
  var url = "jdbc:postgresql://localhost:5432/metastore"
  var user = "metastore"
  var password = "Intel123"

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
    while(resultSet.next()) {
      println("SELECT now() = " + resultSet.getString(1))
      println("Test was SUCCESSFUL")
    }

  }
}
