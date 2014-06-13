package com.intel.intelanalytics.service.v1

import com.intel.intelanalytics.security.UserPrincipal
import org.mockito.Mockito._

import com.intel.intelanalytics.engine.Engine
import scala.concurrent.Future
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.service.{ServiceSpec, CommonDirectives}
import com.intel.intelanalytics.domain.schema.Schema
import org.joda.time.DateTime

class DataFrameServiceSpec extends ServiceSpec {

  implicit val userPrincipal = mock[UserPrincipal]
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("dataframes")).thenReturn(provide(userPrincipal))

  "DataFrameService" should "give an empty set when there are no dataframes" in {

    val engine = mock[Engine]
    val dataFrameService = new DataFrameService(commonDirectives, engine)

    when(engine.getFrames(0, 20)).thenReturn(Future.successful(Seq()))

    Get("/dataframes") ~> dataFrameService.frameRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

  it should "give one dataframe when there is one dataframe" in {
    val engine = mock[Engine]
    val dataFrameService = new DataFrameService(commonDirectives, engine)

    when(engine.getFrames(0, 20)).thenReturn(Future.successful(Seq(DataFrame(1, "name", None, "uri", Schema(), 1, new DateTime(), new DateTime()))))

    Get("/dataframes") ~> dataFrameService.frameRoutes() ~> check {
      assert(responseAs[String] == """[{
                                     |  "id": 1,
                                     |  "name": "name",
                                     |  "url": "http://example.com/dataframes/1"
                                     |}]""".stripMargin)
    }
  }

}
