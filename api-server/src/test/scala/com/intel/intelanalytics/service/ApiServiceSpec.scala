package com.intel.intelanalytics.service

import com.intel.intelanalytics.service.v1.ApiV1Service
import spray.http.StatusCodes

class ApiServiceSpec extends ServiceSpec {

  val apiV1Service = mock[ApiV1Service]
  val apiService = new ApiService(apiV1Service)

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
