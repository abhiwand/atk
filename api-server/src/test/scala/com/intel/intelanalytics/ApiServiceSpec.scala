//package com.intel.intelanalytics
//
//import org.specs2.mutable.Specification
//import spray.testkit.Specs2RouteTest
//import spray.http._
//import StatusCodes._
//import com.intel.intelanalytics.v1.viewmodels.DataFrameHeader
//import com.intel.intelanalytics.service.ApiService
//
//class ApiServiceSpec extends Specification with Specs2RouteTest with ApiService {
//  def actorRefFactory = system
//
//  "ApiService" should {
//
//    "return a greeting for GET requests to the root path" in {
//      Get() ~> serviceRoute ~> check {
//        responseAs[String] must contain("Welcome")
//      }
//    }
//
//    "leave GET requests to other paths unhandled" in {
//      Get("/kermit") ~> serviceRoute ~> check {
//        handled must beFalse
//      }
//    }
//
//    "return a MethodNotAllowed error for PUT requests to the root path" in {
//      Put() ~> sealRoute(serviceRoute) ~> check {
//        status === MethodNotAllowed
//        responseAs[String] === "HTTP method not allowed, supported methods: GET"
//      }
//    }
//  }
//}
