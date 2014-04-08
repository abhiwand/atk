//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._

import com.intel.intelanalytics.v1.viewmodels.DataFrameHeader
import com.intel.intelanalytics.v1.ApiV1Service
import com.intel.intelanalytics.domain.{Schema, DataFrame, FrameComponent}
import org.specs2.mock.Mockito
import com.intel.intelanalytics.repository.Repository
import com.intel.intelanalytics.service.Boot

class V1DataFrameServiceSpec extends Specification with Specs2RouteTest { override def is =
  s""""
    DataFrame service should:
      return an empty JSON list when there are no frames       ${new Svc().empty()}
      return a list of one when there is one                   ${new Svc().one()}
  """
  class Svc extends Boot.V1 with Mockito {
    import com.intel.intelanalytics.v1.viewmodels.ViewModelJsonProtocol._
    override val frameRepo : Repository[Session, DataFrame] = mock[Repository[Session, DataFrame]]

    def empty() = {
      frameRepo.scan(offset = anyInt, count = anyInt)(any[Session]) returns List()
      Get("/dataframes") ~> apiV1Service ~> check {
        responseAs[List[DataFrameHeader]] === List()
      }
    }

    def one() {
      val frame = DataFrame(id = Some(35), name = "foo", schema = Schema(columns = List(("a", "int"))))
      frameRepo.scan(offset = anyInt, count = anyInt)(any[Session]) returns List(frame)
      Get("/dataframes") ~> apiV1Service ~> check {
        responseAs[List[DataFrameHeader]] === List(DataFrameHeader(id = 35, name = "foo",
          url = "http://example.com/dataframes/35"))
      }
    }
  }
}