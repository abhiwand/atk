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

import com.intel.intelanalytics.domain.frame.{ FrameReferenceManagement, FrameReference }
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.engine.spark.command.{ Dependencies }
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext
import org.scalatest.{ FlatSpec, Matchers }
import spray.json._

class DependenciesTest extends FlatSpec with Matchers {

  "getUriReferencesForJson" should "find UriReferences in case classes" in {
    case class Foo(frameId: Int, frame: FrameReference)
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    implicit val fmt = jsonFormat2(Foo)

    val reference = List(FrameReference(3))
    implicit val invocation: Invocation = Call(null, EngineExecutionContext.global)
    FrameReferenceManagement //reference to ensure it's loaded and registered
    Dependencies.getUriReferencesFromJsObject(Foo(1, reference.head).toJson.asJsObject) should be(reference)
  }
}
