package com.intel.intelanalytics.service.v1

import org.mockito.Mockito._
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.service.{ ServiceSpec, CommonDirectives }
import com.intel.intelanalytics.engine.Engine
import scala.concurrent.Future

class CommandServiceSpec extends ServiceSpec {

  implicit val userPrincipal = mock[UserPrincipal]
  val commonDirectives = mock[CommonDirectives]
  when(commonDirectives.apply("commands")).thenReturn(provide(userPrincipal))

  "CommandService" should "give an empty set when there are no results" in {

    val engine = mock[Engine]
    val commandService = new CommandService(commonDirectives, engine)

    when(engine.getCommands(0, 20)).thenReturn(Future.successful(Seq()))

    Get("/commands") ~> commandService.commandRoutes() ~> check {
      assert(responseAs[String] == "[]")
    }
  }

}
