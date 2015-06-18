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

import com.intel.intelanalytics.domain.query.{ QueryDataResult, Query }
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.rest.threading.SprayExecutionContext
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.rest.{ CommonDirectives, ServiceTest }
import org.joda.time.DateTime
import org.mockito.Mockito._
import scala.concurrent.duration._

import scala.concurrent.Future

class QueryServiceTest extends ServiceTest {

  // increasing timeout because this test had intermittent failures on build server
  implicit val routeTestTimeout = RouteTestTimeout(10.seconds)

  implicit val userPrincipal = mock[UserPrincipal]
  implicit val call: Invocation = Call(userPrincipal, SprayExecutionContext.global)
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("queries")).thenReturn(provide(call))

  "QueryService" should "give an empty set when there are no results" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQueries(0, 20)).thenReturn(Future.successful(Seq()))

    Get("/queries") ~> queryService.queryRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

  "QueryService" should "give one query when there is one query" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQueries(0, 20)).thenReturn(
      Future.successful(Seq(Query(1, "frames/data", None, None, false, Some(5), Some(10), new DateTime(), new DateTime(), None))))

    Get("/queries") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      assert(r == """[{
                    |  "id": 1,
                    |  "name": "frames/data",
                    |  "url": "http://example.com/queries/1"
                    |}]""".stripMargin)
    }
  }

  "QueryService" should "give details on query when it is requested" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQuery(1)).thenReturn(
      Future.successful(Some(Query(1, "frames/data", None, None, false, Some(5), None, new DateTime(), new DateTime(), None))))

    Get("/queries/1") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      val expected = """{
                       |  "id": 1,
                       |  "name": "frames/data",
                       |  "complete": false,
                       |  "links": [{
                       |    "rel": "self",
                       |    "uri": "http://example.com/queries/1",
                       |    "method": "GET"
                       |  }],
                       |  "correlation_id": ""
                       |}""".stripMargin
      assert(r == expected)
    }
  }

  "QueryService" should "give a list of partitions when requesting query partitions" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQuery(1)).thenReturn(
      Future.successful(Some(Query(1, "frames/data", None, None, true, Some(5), None, new DateTime(), new DateTime(), None))))

    Get("/queries/1/data") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      assert(r == """[{
                    |  "id": 1,
                    |  "url": "http://example.com/queries/1/data/1"
                    |}, {
                    |  "id": 2,
                    |  "url": "http://example.com/queries/1/data/2"
                    |}, {
                    |  "id": 3,
                    |  "url": "http://example.com/queries/1/data/3"
                    |}, {
                    |  "id": 4,
                    |  "url": "http://example.com/queries/1/data/4"
                    |}, {
                    |  "id": 5,
                    |  "url": "http://example.com/queries/1/data/5"
                    |}]""".stripMargin)
    }
  }

  "QueryService" should "give the data when requesting query partitions" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQuery(1)).thenReturn(
      Future.successful(Some(Query(1, "frames/data", None, None, true, Some(5), None, new DateTime(), new DateTime(), None))))

    when(engine.getQueryPage(1, 0)).thenReturn(
      QueryDataResult(List(), None)
    )

    Get("/queries/1/data/1") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      assert(r == """{
                    |  "id": 1,
                    |  "name": "frames/data",
                    |  "complete": true,
                    |  "result": {
                    |    "data": [],
                    |    "page": 1,
                    |    "total_pages": 5
                    |  },
                    |  "links": [{
                    |    "rel": "self",
                    |    "uri": "http://example.com/queries/1/data/1",
                    |    "method": "GET"
                    |  }],
                    |  "correlation_id": ""
                    |}""".stripMargin)
    }
  }

}
