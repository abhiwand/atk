package com.intel.intelanalytics.engine.spark.graph

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito

import org.apache.hadoop.hbase.client.HBaseAdmin

class SparkGraphHBaseBackendSpec extends Specification with Mockito {

  "Deleting a table that does not exist" should {
    "throw an illegal argument exception" in {

      val tableName = "table for none"
      val mockHBaseAdmin = mock[HBaseAdmin]
      mockHBaseAdmin.tableExists(tableName) returns false

      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(mockHBaseAdmin)

      sparkGraphHBaseBackend.deleteUnderlyingTable(tableName) must throwA[IllegalArgumentException];

    }
  }


  "Deleting a table that does exist" should {
    "cause table to be disabled and deleted" in {
      val userTableName = "mytable"
      val internalTableName = "iat_graph_mytable"
      val mockHBaseAdmin = mock[HBaseAdmin]
      mockHBaseAdmin.tableExists(internalTableName) returns true

      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(mockHBaseAdmin)
      sparkGraphHBaseBackend.deleteUnderlyingTable(userTableName)

      there was one(mockHBaseAdmin).disableTable(internalTableName)
      there was one(mockHBaseAdmin).deleteTable(internalTableName)
    }
  }
}
