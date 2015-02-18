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

package com.intel.intelanalytics.spray.json

import org.scalatest.{ FlatSpec, Matchers }

import spray.json._
import IADefaultJsonProtocol._

class IADefaultJsonProtocolTest extends FlatSpec with Matchers {

  /** For testing case class toJson and back */
  case class Foo(lower: String, mixedCase: String, under_score: String)

  case class Bar(fooA: Foo, fooB: Foo)

  implicit val fooFormat = jsonFormat3(Foo)
  implicit val barFormat = jsonFormat2(Bar)

  "CustomJsonProtocol" should "support converting a case class using camelCase to JSON with underscores" in {
    val foo = Foo("AAA", "BBB", "CCC")
    val json = foo.toJson.compactPrint
    assert(json.contains("mixed_case"), "JSON did not contain underscores: " + json)
  }

  it should "support reading JSON with underscores and converting to camelCase names in a case class" in {
    val json = JsonParser("{\"lower\":\"AAA\",\"mixed_case\":\"BBB\",\"under_score\":\"CCC\"}")
    val foo = json.convertTo[Foo]
    assert(foo.lower == "AAA")
    assert(foo.mixedCase == "BBB")
    assert(foo.under_score == "CCC")
  }

  it should "support reading JSON with randomly ordered properties" in {
    val json = JsonParser("{\"mixed_case\":\"BBB\",\"under_score\":\"CCC\",\"lower\":\"AAA\"}")
    val foo = json.convertTo[Foo]
    assert(foo.lower == "AAA")
    assert(foo.mixedCase == "BBB")
    assert(foo.under_score == "CCC")
  }

  it should "support converting nested case classes to JSON" in {
    val fooA = Foo("AAA", "BBB", "CCC")
    val fooB = Foo("111", "222", "333")
    val bar = Bar(fooA, fooB)
    val json = bar.toJson.prettyPrint
    assert(json == """{
                     |  "foo_a": {
                     |    "lower": "AAA",
                     |    "mixed_case": "BBB",
                     |    "under_score": "CCC"
                     |  },
                     |  "foo_b": {
                     |    "lower": "111",
                     |    "mixed_case": "222",
                     |    "under_score": "333"
                     |  }
                     |}""".stripMargin)
  }

  it should "support converting JSON to nested case classes" in {
    val json = JsonParser("""{
                             |  "foo_a": {
                             |    "lower": "AAA",
                             |    "mixed_case": "BBB",
                             |    "under_score": "CCC"
                             |  },
                             |  "foo_b": {
                             |    "lower": "111",
                             |    "mixed_case": "222",
                             |    "under_score": "333"
                             |  }
                             |}""".stripMargin)

    val fooA = Foo("AAA", "BBB", "CCC")
    val fooB = Foo("111", "222", "333")
    val bar = Bar(fooA, fooB)
    assert(json.convertTo[Bar] == bar)
  }

}
