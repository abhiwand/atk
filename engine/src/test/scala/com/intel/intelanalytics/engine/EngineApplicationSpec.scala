package com.intel.intelanalytics.engine

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito


class EngineApplicationSpec extends Specification with Mockito {

  "EngineApplication" should {

    val engineApplication = new EngineApplication() {
      engine = new EngineComponent with FrameComponent {
        override def engine: Engine = mock[Engine]
        override def frames: FrameStorage = mock[FrameStorage]
      }
    }

    // these unit tests are a good example of how it is difficult to bolt
    // on unit tests after the code has been written

    "get an instance from the description" in {
      engineApplication.get[Any]("engine") mustNotEqual null
    }

    "throw error on bad description" in {
      engineApplication.get[Any]("badValue") must throwA[IllegalArgumentException]
    }

    "should throw an error when  it can't be started" in {
      engineApplication.start(null) must throwA[RuntimeException]
    }

  }
}
