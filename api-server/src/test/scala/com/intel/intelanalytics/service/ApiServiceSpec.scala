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

package com.intel.intelanalytics.service

import com.intel.intelanalytics.service.v1.ApiV1Service
import com.intel.intelanalytics.service.{ AuthenticationDirective, CommonDirectives }

class ApiServiceSpec extends ServiceSpec {

  val apiV1Service = mock[ApiV1Service]
  val authenticationDirective = mock[AuthenticationDirective]
  val commonDirectives = new CommonDirectives(authenticationDirective)
  val apiService = new ApiService(commonDirectives, apiV1Service)

  "ApiService" should "return a greeting for GET requests to the root path" in {
    Get() ~> apiService.serviceRoute ~> check {
      assert(responseAs[String].contains("Welcome to the Intel Analytics Toolkit API Server"))
      assert(status.intValue == 200)
      assert(status.isSuccess)
    }
  }

  it should "provide version info as JSON" in {
    Get("/info") ~> apiService.serviceRoute ~> check {
      assert(responseAs[String] == """{
                                     |  "name": "Intel Analytics",
                                     |  "identifier": "ia",
                                     |  "versions": ["v1"]
                                     |}""".stripMargin)
      assert(status.intValue == 200)
      assert(status.isSuccess)
    }
  }

}
