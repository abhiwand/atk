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

import java.util.Date
import org.scalatest.{ Matchers, WordSpec }

class InputRowTest extends WordSpec with Matchers {

  val date = new Date()
  val inputSchema = new InputSchema(List("col1", "col2", "dateCol", "emptyStringCol", "nullColumn", "emptyList", "complexObject"), null)
  val values = List("abc", "def", date, "", null, Nil, Map("key" -> "value"))
  val inputRow = new InputRow(inputSchema, values)

  "InputRow" should {

    "handle String values in the input" in {
      inputRow.value("col1") shouldBe "abc"
    }

    "handle Date values in the input" in {
      inputRow.value("dateCol") shouldBe date
    }

    "handle null values in the input" in {
      assert(inputRow.value("nullColumn") == null)
    }

    "handle List values in the input" in {
      inputRow.value("emptyList") shouldBe Nil
    }

    "handle complex object values in the input" in {
      inputRow.value("complexObject") shouldBe Map("key" -> "value")
    }

    "consider a non-empty String to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("col1") shouldBe true
    }

    "consider a Date object to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("dateCol") shouldBe true
    }

    "consider the empty string to be an empty column value" in {
      inputRow.columnIsNotEmpty("emptyStringCol") shouldBe false
    }

    "consider null to be an empty column value" in {
      inputRow.columnIsNotEmpty("nullColumn") shouldBe false
    }

    "consider the empty list to be an empty column value" in {
      inputRow.columnIsNotEmpty("emptyList") shouldBe false
    }

    "consider complex objects to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("complexObject") shouldBe true
    }

    "throw an exception for non-existant column names when getting value" in {
      an[NoSuchElementException] should be thrownBy inputRow.value("no-column-with-this-name")
    }

    "throw an exception for non-existant column names when checking for empty" in {
      an[NoSuchElementException] should be thrownBy inputRow.columnIsNotEmpty("no-column-with-this-name")
    }

    "throw an exception when the number of columns in schema greater than the number in the row" in {
      val inputSchema = new InputSchema(List("col1", "col2", "col3"), null)
      val values = List("abc", "def")
      an[IllegalArgumentException] should be thrownBy new InputRow(inputSchema, values)
    }

    "throw an exception when the number of columns in schema is less than the number in the row" in {
      val inputSchema = new InputSchema(List("col1", "col2"), null)
      val values = List("abc", "def", "ghi")
      an[IllegalArgumentException] should be thrownBy new InputRow(inputSchema, values)
    }
  }
}
