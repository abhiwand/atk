//package com.intel.intelanalytics.engine
//
//import org.specs2.mutable.Specification
//import org.specs2.mock.Mockito
//import com.intel.intelanalytics.domain.frame.DataFrame
//import java.util.concurrent.Future
//
//class EngineActorSpec extends Specification with Mockito {
//  "EngineActor" should {
//    val engine = new EngineComponent {
//      override def engine: Engine = mock[Engine]
//
//      val id: Identifier = mock[Identifier]
//      val frame = engine.getFrame(id)
//      "get a frame" in {
//        val frame = engine.getFrame(id)
//        frame mustNotEqual null
//      }
//      "return a data frame" in {
//        val file: String = mock[String]
//        def parser: Functional = mock[Functional]
//        def frame = mock[DataFrame]
//        val appendedFile = engine.appendFile(frame, file, parser)
//        appendedFile mustNotEqual null
//      }
//      "warn about unknown message" in {
//        engine.appendFile(mock[DataFrame], mock[String], mock[Functional]) equals null
//      }
//    }
//  }
//}