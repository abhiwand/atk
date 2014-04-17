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

package com.intel.graphbuilder.parser

import java.util.Date
import org.specs2.mutable.Specification

class InputRowSpec extends Specification {

  val date = new Date()
  val inputSchema = new InputSchema(List("col1", "col2", "dateCol", "emptyStringCol", "nullColumn", "emptyList", "complexObject"), null)
  val values = List("abc", "def", date, "", null, Nil, Map("key" -> "value"))
  val inputRow = new InputRow(inputSchema, values)

  "InputRow" should {

    "handle String values in the input" in {
      inputRow.value("col1") mustEqual "abc"
    }

    "handle Date values in the input" in {
      inputRow.value("dateCol") mustEqual date
    }

    "handle null values in the input" in {
      inputRow.value("nullColumn") mustEqual null
    }

    "handle List values in the input" in {
      inputRow.value("emptyList") mustEqual Nil
    }

    "handle complex object values in the input" in {
      inputRow.value("complexObject") mustEqual Map("key" -> "value")
    }

    "consider a non-empty String to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("col1") mustEqual true
    }

    "consider a Date object to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("dateCol") mustEqual true
    }

    "consider the empty string to be an empty column value" in {
      inputRow.columnIsNotEmpty("emptyStringCol") mustEqual false
    }

    "consider null to be an empty column value" in {
      inputRow.columnIsNotEmpty("nullColumn") mustEqual false
    }

    "consider the empty list to be an empty column value" in {
      inputRow.columnIsNotEmpty("emptyList") mustEqual false
    }

    "consider complex objects to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("complexObject") mustEqual true
    }

    "throw an exception for non-existant column names when getting value" in {
      inputRow.value("no-column-with-this-name") must throwA[NoSuchElementException]
    }

    "throw an exception for non-existant column names when checking for empty" in {
      inputRow.columnIsNotEmpty("no-column-with-this-name") must throwA[NoSuchElementException]
    }

    "throw an exception when the number of columns in schema greater than the number in the row" in {
      val inputSchema = new InputSchema(List("col1", "col2", "col3"), null)
      val values = List("abc", "def")
      new InputRow(inputSchema, values) must throwA[IllegalArgumentException]
    }

    "throw an exception when the number of columns in schema is less than the number in the row" in {
      val inputSchema = new InputSchema(List("col1", "col2"), null)
      val values = List("abc", "def", "ghi")
      new InputRow(inputSchema, values) must throwA[IllegalArgumentException]
    }
  }
}
