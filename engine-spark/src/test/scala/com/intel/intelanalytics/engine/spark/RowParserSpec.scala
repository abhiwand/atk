package com.intel.intelanalytics.engine.spark

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

import org.specs2.mutable.Specification
import com.intel.intelanalytics.engine.RowParser

class RowParserSpec extends Specification {
  val csvRowParser = new RowParser(',')
  "RowParser" should {
    "parse a String" in {
      csvRowParser.apply("a,b") shouldEqual Array("a", "b")
    }
  }
  "RowParser" should {
    "parse a String with single quotes" in {
      csvRowParser.apply("foo and bar,bar and foo,'foo, is bar'") shouldEqual Array("foo and bar", "bar and foo", "foo, is bar")
    }
  }
  "RowParser" should {
    "parse an empty string" in {
      csvRowParser.apply("") shouldEqual Array("")
    }
  }
  "RowParser" should {
    "parse a nested double quotes string" in {
      csvRowParser.apply("foo and bar,bar and foo,\"foo, is bar\"") shouldEqual Array("foo and bar", "bar and foo", "foo, is bar")
    }
  }
  "RowParser" should {
    "parse a string with empty fields" in {
      csvRowParser.apply("foo,bar,,,baz") shouldEqual Array("foo", "bar", "", "", "baz")
    }
  }
  val trow = new RowParser('\t')
  "RowParser" should {
    "parse a tab separated string" in {
      trow.apply("foo\tbar\tbaz") shouldEqual Array("foo", "bar", "baz")
    }
  }

}

