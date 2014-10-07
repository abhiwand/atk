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

import com.intel.intelanalytics.security.UserPrincipal
import org.mockito.Mockito._

import com.intel.intelanalytics.engine.Engine
import scala.concurrent.Future
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.service.{ ServiceTest, CommonDirectives }
import com.intel.intelanalytics.domain.schema.Schema
import org.joda.time.DateTime

class DataFrameServiceTest extends ServiceTest {

  implicit val userPrincipal = mock[UserPrincipal]
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("frames")).thenReturn(provide(userPrincipal))

  "DataFrameService" should "give an empty set when there are no frames" in {

    val engine = mock[Engine]
    val dataFrameService = new DataFrameService(commonDirectives, engine)

    when(engine.getFrames()).thenReturn(Future.successful(Seq()))

    Get("/frames") ~> dataFrameService.frameRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

  it should "give one dataframe when there is one dataframe" in {
    val engine = mock[Engine]
    val dataFrameService = new DataFrameService(commonDirectives, engine)

    when(engine.getFrames()).thenReturn(Future.successful(Seq(DataFrame(1, "name", None, Schema(), 0, 1, new DateTime(), new DateTime()))))

    Get("/frames") ~> dataFrameService.frameRoutes() ~> check {
      assert(responseAs[String] == """[{
                                     |  "id": 1,
                                     |  "name": "name",
                                     |  "url": "http://example.com/frames/1"
                                     |}]""".stripMargin)
    }
  }

}
