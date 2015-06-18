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

package com.intel.intelanalytics.rest.v1

import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.rest.threading.SprayExecutionContext
import com.intel.intelanalytics.security.UserPrincipal
import org.mockito.Mockito._

import com.intel.intelanalytics.engine.Engine
import scala.concurrent.Future
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.rest.{ ServiceTest, CommonDirectives }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Schema }
import org.joda.time.DateTime

class FrameServiceTest extends ServiceTest {

  implicit val userPrincipal = mock[UserPrincipal]
  implicit val call: Invocation = Call(userPrincipal, SprayExecutionContext.global)
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("frames")).thenReturn(provide(call))
  "DataFrameService" should "give an empty set when there are no frames" in {

    val engine = mock[Engine]
    val dataFrameService = new FrameService(commonDirectives, engine)

    when(engine.getFrames()).thenReturn(Future.successful(Seq()))

    Get("/frames") ~> dataFrameService.frameRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

  it should "give one dataframe when there is one dataframe" in {
    val engine = mock[Engine]
    val dataFrameService = new FrameService(commonDirectives, engine)

    when(engine.getFrames()).thenReturn(Future.successful(Seq(FrameEntity(1, Some("name"), FrameSchema(), 1, new DateTime, new DateTime))))

    Get("/frames") ~> dataFrameService.frameRoutes() ~> check {
      assert(responseAs[String] == """[{
                                     |  "id": 1,
                                     |  "name": "name",
                                     |  "url": "http://example.com/frames/1",
                                     |  "entity_type": "frame:"
                                     |}]""".stripMargin)
    }
  }

}
