package com.intel.intelanalytics.service

import com.intel.intelanalytics.service.v1.ApiV1Service

class ApiServiceSpec extends ServiceSpec {

  val apiV1Service = mock[ApiV1Service]
  val apiService = new ApiService(apiV1Service)

  "ApiService" should "return a greeting for GET requests to the root path" in {
    Get() ~> apiService.serviceRoute ~> check {
      responseAs[String] should include("Welcome to the Intel Analytics Toolkit API Server")
    }
  }

  it should "provide version info as JSON" in {
    Get("/info") ~> apiService.serviceRoute ~> check {
      responseAs[String] should be(
        """{
          |  "name": "Intel Analytics",
          |  "identifier": "ia",
          |  "versions": ["v1"]
          |}""".stripMargin)
    }
  }

}
