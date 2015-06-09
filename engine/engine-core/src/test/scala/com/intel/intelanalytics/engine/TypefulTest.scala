/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
