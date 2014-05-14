//package com.intel.intelanalytics.service
//
//import org.specs2.mutable.Specification
//import com.intel.intelanalytics.service.v1.{ V1DataFrameService, ApiV1Service }
//import com.intel.intelanalytics.repository.MetaStoreComponent
//import com.intel.intelanalytics.engine.EngineComponent
//import org.specs2.mock.Mockito
//import spray.testkit.Specs2RouteTest
//import spray.routing.HttpService
//import spray.http.StatusCodes._
//import akka.actor.ActorRefFactory
//import com.intel.intelanalytics.domain.{ Schema, DataFrame }
//import scala.concurrent.Future
//import scala.util.Try
//
//class ApiServiceSpec extends Specification with Specs2RouteTest with HttpService with Mockito {
//  def actorRefFactory: ActorRefFactory = system
//
//  val apiService = new ApiService with ApiV1Service with V1DataFrameService with MetaStoreComponent with EngineComponent {
//    override val metaStore: MetaStore = mock[MetaStore]
//
//    override def engine: Engine = mock[Engine]
//
//    //val future = Future.successful(Seq(mock[DataFrame]))
//    //engine.getFrames(0,20) returns future
//  }
//
//  "ApiService" should {
//
//    "return a greeting for GET requests to the root path" in {
//      Get() ~> apiService.serviceRoute ~> check {
//        responseAs[String] must contain("Welcome to the Intel Analytics Toolkit API Server")
//      }
//    }
//
//    "provide version info as json " in {
//      Get("/info") ~> apiService.serviceRoute ~> check {
//        responseAs[String] must contain("Intel Analytics")
//        responseAs[String] must contain("ia")
//        responseAs[String] must contain("v1")
//
//      }
//    }
//
//    "return a greeting for GET requests to the root path" in {
//      Get("/v1/dataframes") ~> apiService.serviceRoute ~> check {
//        responseAs[String] must contain("Welcome to the Intel Analytics Toolkit API Server")
//      }
//
//    }
//  }
//}
