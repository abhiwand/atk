package com.intel.intelanalytics.engine.spark.graph

import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.scalatest.mock.MockitoSugar

class SparkGraphHBaseBackendTest extends WordSpec with Matchers with MockitoSugar {

  "Not quietly deleting a table that does not exist" should {
    "throw an illegal argument exception" in {

      val tableName = "table for none"
      val mockHBaseAdmin = mock[HBaseAdmin]
      when(mockHBaseAdmin.tableExists(tableName)).thenReturn(false)

      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(mockHBaseAdmin)

      an[IllegalArgumentException] should be thrownBy sparkGraphHBaseBackend.deleteUnderlyingTable(tableName, quiet = false)

    }
  }

  "Quietly deleting a table that does exist" should {
    "cause table to be disabled and deleted" in {
      val userTableName = "mytable"
      val internalTableName = "iat_graph_mytable"
      val mockHBaseAdmin = mock[HBaseAdmin]
      when(mockHBaseAdmin.tableExists(internalTableName)).thenReturn(true)
      when(mockHBaseAdmin.isTableEnabled(internalTableName)).thenReturn(true)

      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(mockHBaseAdmin)
      sparkGraphHBaseBackend.deleteUnderlyingTable(userTableName, quiet = true)

      verify(mockHBaseAdmin, times(1)).disableTable(internalTableName)
      verify(mockHBaseAdmin, times(1)).deleteTable(internalTableName)
    }
  }
}
