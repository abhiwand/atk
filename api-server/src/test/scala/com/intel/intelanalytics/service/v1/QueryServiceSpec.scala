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

package com.intel.intelanalytics.service.v1

import com.intel.intelanalytics.domain.query.Query
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.service.{ CommonDirectives, ServiceSpec }
import org.joda.time.DateTime
import org.mockito.Mockito._

import scala.concurrent.Future

class QueryServiceSpec extends ServiceSpec {

  implicit val userPrincipal = mock[UserPrincipal]
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("queries")).thenReturn(provide(userPrincipal))

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
      Future.successful(Seq(Query(1, "dataframes/data", None, None, List(1), List(), false, Some(5), Some(10), new DateTime(), new DateTime(), None))))

    Get("/queries") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      assert(r == """[{
                    |  "id": 1,
                    |  "name": "dataframes/data",
                    |  "url": "http://example.com/queries/1"
                    |}]""".stripMargin)
    }
  }

  "QueryService" should "give details on query when it is requested" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQuery(1)).thenReturn(
      Future.successful(Some(Query(1, "dataframes/data", None, None, List(1), List(), false, Some(5), None, new DateTime(), new DateTime(), None))))

    Get("/queries/1") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      assert(r == """{
                    |  "id": 1,
                    |  "name": "dataframes/data",
                    |  "progress": [1.0],
                    |  "progressMessage": [],
                    |  "complete": false,
                    |  "links": [{
                    |    "rel": "self",
                    |    "uri": "http://example.com/queries/1",
                    |    "method": "GET"
                    |  }]
                    |}""".stripMargin)
    }
  }

  "QueryService" should "give a list of partitions when requesting query partitions" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQuery(1)).thenReturn(
      Future.successful(Some(Query(1, "dataframes/data", None, None, List(1), List(), true, Some(5), None, new DateTime(), new DateTime(), None))))

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

  "QueryService" should "give a the data when requesting query partitions" in {

    val engine = mock[Engine]
    val queryService = new QueryService(commonDirectives, engine)

    when(engine.getQuery(1)).thenReturn(
      Future.successful(Some(Query(1, "dataframes/data", None, None, List(1), List(), true, Some(5), None, new DateTime(), new DateTime(), None))))

    when(engine.getQueryPage(1, 0)).thenReturn(
      List()
    )

    Get("/queries/1/data/1") ~> queryService.queryRoutes() ~> check {
      val r = responseAs[String]
      assert(r == """{
                    |  "id": 1,
                    |  "name": "dataframes/data",
                    |  "progress": [1.0],
                    |  "progressMessage": [],
                    |  "complete": true,
                    |  "result": {
                    |    "data": [],
                    |    "pages": 1,
                    |    "totalPages": 5
                    |  },
                    |  "links": [{
                    |    "rel": "self",
                    |    "uri": "http://example.com/queries/1/data/1",
                    |    "method": "GET"
                    |  }]
                    |}""".stripMargin)
    }
  }

}
