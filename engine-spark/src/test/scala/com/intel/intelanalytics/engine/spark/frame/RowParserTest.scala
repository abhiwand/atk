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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.DataTypes
import org.scalatest.{ Matchers, WordSpec }

class RowParserTest extends WordSpec with Matchers {

  val csvRowParser = new RowParser(',', Array[DataTypes.DataType]())

  "RowParser" should {

    "parse a String" in {
      csvRowParser.splitLineIntoParts("a,b") shouldEqual Array("a", "b")
    }

    "parse an empty string" in {
      csvRowParser.splitLineIntoParts("") shouldEqual Array()
    }

    "parse a nested double quotes string" in {
      csvRowParser.splitLineIntoParts("foo and bar,bar and foo,\"foo, is bar\"") shouldEqual Array("foo and bar", "bar and foo", "foo, is bar")
    }

    "remove double quotes" in {
      csvRowParser.splitLineIntoParts("\"foo\",\"bar\"") shouldEqual Array("foo", "bar")
    }

    "not do anything special with single quotes" in {
      csvRowParser.splitLineIntoParts("'foo','bar'") shouldEqual Array("'foo'", "'bar'")
    }

    "parse a string with empty fields" in {
      csvRowParser.splitLineIntoParts("foo,bar,,,baz") shouldEqual Array("foo", "bar", "", "", "baz")
    }

    "preserve leading and trailing tab/s in a string" in {
      csvRowParser.splitLineIntoParts("\tfoo,bar,baz") shouldEqual Array("\tfoo", "bar", "baz")
    }

    "preserve leading and trailing spaces in a string" in {
      csvRowParser.splitLineIntoParts(" foo,bar ,baz") shouldEqual Array(" foo", "bar ", "baz")
    }

    "parse a tab separated string" in {
      val trow = new RowParser('\t', Array[DataTypes.DataType]())
      trow.splitLineIntoParts("foo\tbar\tbaz") shouldEqual Array("foo", "bar", "baz")
    }
  }
}

