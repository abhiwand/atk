//package com.intel.intelanalytics.engine
//
//import org.specs2.mutable.Specification
//import org.specs2.mock.Mockito
//import com.intel.intelanalytics.domain.frame.DataFrame
//import java.util.concurrent.Future
//
//class EngineActorSpec extends Specification with Mockito {
//  "EngineActor" should {
//    val engine = new EngineComponent
//    {
//      override def engine: Engine = mock[Engine]
//
//      val name: String = mock[String]
//      val frame = engine.getFrameByName(name)
//      "get frame by name" in{
//        val frame = engine.getFrameByName(name)
//        frame mustNotEqual null
//      }
//
//    }
//
//  }
//}