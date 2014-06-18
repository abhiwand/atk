package com.intel.intelanalytics.service.v1

import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.service.{CommonDirectives, ServiceSpec}
import org.mockito.Mockito._

import scala.concurrent.Future

/**
 * Created by rhicke on 6/13/14.
 */
class CommandServiceSpec extends ServiceSpec{

  implicit val userPrincipal = mock[UserPrincipal]
  val commonDirectives = mock[CommonDirectives]

  it should "should parse a file" in {

    val engine = mock[Engine]
    val commandFrameService = new CommandService(commonDirectives, engine)


  }


}
