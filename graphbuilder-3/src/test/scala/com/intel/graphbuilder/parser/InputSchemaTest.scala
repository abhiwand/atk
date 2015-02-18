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

package com.intel.graphbuilder.parser

import org.scalatest.{ Matchers, WordSpec }

class InputSchemaTest extends WordSpec with Matchers {

  "ColumnDef" should {

    "require a non-empty column name" in {
      an[IllegalArgumentException] should be thrownBy new ColumnDef("", null, null)
    }

    "require a non-null column name" in {
      an[IllegalArgumentException] should be thrownBy new ColumnDef(null, null, null)
    }

    "require an index greater than or equal to zero" in {
      an[IllegalArgumentException] should be thrownBy new ColumnDef("any", null, -1)
    }
  }

  "InputSchema" should {

    val columnDefs = List(new ColumnDef("col1", classOf[String], null), new ColumnDef("col2", classOf[Int], null))
    val inputSchema = new InputSchema(columnDefs)

    "keep track of the number of columns" in {
      inputSchema.size shouldBe 2
    }

    "report dataType correctly for String" in {
      inputSchema.columnType("col1") shouldBe classOf[String]
    }

    "report dataType correctly for Int" in {
      inputSchema.columnType("col2") shouldBe classOf[Int]
    }

    "calculate and report the index of 1st column correctly" in {
      inputSchema.columnIndex("col1") shouldBe 0
    }

    "calculate and report the index of 2nd column correctly" in {
      inputSchema.columnIndex("col2") shouldBe 1
    }

    "accept indexes provided as part of the column definition" in {
      val columnDefs = List(new ColumnDef("col1", classOf[String], 1), new ColumnDef("col2", classOf[Int], 0))
      val inputSchema = new InputSchema(columnDefs)
      inputSchema.columnIndex("col1") shouldBe 1
      inputSchema.columnIndex("col2") shouldBe 0
    }
  }
}
