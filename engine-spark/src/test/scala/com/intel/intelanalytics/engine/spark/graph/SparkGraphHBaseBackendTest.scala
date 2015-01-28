package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.engine.plugin.Call
import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.scalatest.mock.MockitoSugar

class SparkGraphHBaseBackendTest extends WordSpec with Matchers with MockitoSugar {

  implicit val call = Call(null)
//TODO: enable the test when TRIB-4413 is fixed
//  "Not quietly deleting a table that does not exist" should {
//    "throw an illegal argument exception" in {
//
//      val tableName = "table for none"
//      val mockHBaseAdmin = mock[HBaseAdmin]
//      when(mockHBaseAdmin.tableExists(tableName)).thenReturn(false)
//
//      val hbaseAdminFactory = mock[HBaseAdminFactory]
//      when(hbaseAdminFactory.createHBaseAdmin()).thenReturn(mockHBaseAdmin)
//
//      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory)
//
//      an[IllegalArgumentException] should be thrownBy sparkGraphHBaseBackend.deleteUnderlyingTable(tableName, quiet = false)
//
//    }
//  }

  "Quietly deleting a table that does exist" should {
    "cause table to be disabled and deleted" in {
      val userTableName = "mytable"
      val internalTableName = "iat_graph_mytable"
      val mockHBaseAdmin = mock[HBaseAdmin]
      when(mockHBaseAdmin.tableExists(internalTableName)).thenReturn(true)
      when(mockHBaseAdmin.isTableEnabled(internalTableName)).thenReturn(true)

      val hbaseAdminFactory = mock[HBaseAdminFactory]
      when(hbaseAdminFactory.createHBaseAdmin()).thenReturn(mockHBaseAdmin)

      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory)

      sparkGraphHBaseBackend.deleteUnderlyingTable(userTableName, quiet = true)

      verify(mockHBaseAdmin, times(1)).disableTable(internalTableName)
      verify(mockHBaseAdmin, times(1)).deleteTable(internalTableName)
    }
  }
}
