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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.engine.spark.command.Typeful
import org.scalatest.{ FlatSpec, Matchers }

import spray.json._

class TypefulTest extends FlatSpec with Matchers {

  import Typeful.Searchable._

  case class Foo(bar: Int, baz: String)
  case class Bar(oog: Double, gnort: String, foos: List[Foo])
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val fmt = jsonFormat2(Foo)
  implicit val fm2 = jsonFormat3(Bar)

  "deepFind" should "find things in lists" in {
    List(Foo(3, "assiduous"), Foo(4, "benign")).deepFind((s: String) => true) should be(List("assiduous", "benign"))
  }

  "deepFind" should "find things in case classes" in {
    Foo(3, "assiduous").deepFind((s: String) => s.endsWith("ous")) should be(Seq("assiduous"))
  }

  "deepFind" should "find strings in JsObjects" in {
    Foo(3, "assiduous").toJson.asJsObject.deepFind((s: String) => s.endsWith("ous")) should be(Seq("assiduous"))
  }

  "deepFind" should "find numbers in JsObjects" in {
    Foo(3, "assiduous").toJson.asJsObject.deepFind((n: Int) => n == 3) should be(Seq(3))
  }

  //Doesn't work yet, but would be nice.
  //  "deepFind" should "find JsObjects in JsObjects" in {
  //    Bar(2.3, "hello", List(Foo(3, "assiduous"), Foo(8, "world"))).toJson
  //      .deepFind((o: JsObject) => o.getFields("bar").exists { case JsString(s) => s.endsWith("ous")}) should be(Seq(Foo(3, "assiduous")))
  //  }

  "deepFind" should "find JsStrings in JsObjects" in {
    Foo(3, "assiduous").toJson.asJsObject.deepFind((s: JsString) => s.value.endsWith("ous")) should be(Seq(JsString("assiduous")))
  }

  "deepFind" should "find things themselves" in {
    "assiduous".deepFind((s: String) => s.endsWith("ous")) should be(Seq("assiduous"))
  }

}
