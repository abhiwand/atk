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

package com.intel.taproot.analytics.engine.frame.plugins

import com.intel.taproot.analytics.engine.frame.MiscFrameFunctions
import org.scalatest.{ FlatSpec, Matchers }

class DropDuplicatesArgsTest extends FlatSpec with Matchers {
  "createKeyValuePairFromRow" should "include specified 2 key columns as key" in {

    val t = MiscFrameFunctions.createKeyValuePairFromRow(Array[Any]("John", 1, "Titanic"), Seq(0, 1))
    t._1 shouldBe Seq("John", 1)
    t._2 shouldBe Array[Any]("John", 1, "Titanic")
  }
}
