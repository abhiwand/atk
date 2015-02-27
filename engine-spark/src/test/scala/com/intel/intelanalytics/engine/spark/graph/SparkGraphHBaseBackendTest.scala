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

  //  "Quietly deleting a table that does exist" should {
  //    "cause table to be disabled and deleted" in {
  //      val userTableName = "mytable"
  //      val internalTableName = "iat_graph_mytable"
  //      val mockHBaseAdmin = mock[HBaseAdmin]
  //      when(mockHBaseAdmin.tableExists(internalTableName)).thenReturn(true)
  //      when(mockHBaseAdmin.isTableEnabled(internalTableName)).thenReturn(true)
  //
  //      val hbaseAdminFactory = mock[HBaseAdminFactory]
  //      when(hbaseAdminFactory.createHBaseAdmin()).thenReturn(mockHBaseAdmin)
  //
  //      val sparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory)
  //
  //      sparkGraphHBaseBackend.deleteUnderlyingTable(userTableName, quiet = true)
  //
  //      verify(mockHBaseAdmin, times(1)).disableTable(internalTableName)
  //      verify(mockHBaseAdmin, times(1)).deleteTable(internalTableName)
  //    }
  //  }
}
