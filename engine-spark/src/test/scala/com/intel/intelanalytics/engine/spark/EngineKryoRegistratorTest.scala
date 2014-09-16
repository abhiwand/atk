package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.mock.MockitoSugar
import com.esotericsoftware.kryo.Kryo
import org.mockito.Mockito._
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, RowParseResult }

class EngineKryoRegistratorTest extends WordSpec with Matchers with MockitoSugar {

  "EngineKryoRegistrator" should {

    "register expected classes" in {

      val kryo = mock[Kryo]

      // invoke method under test
      new EngineKryoRegistrator().registerClasses(kryo)

      verify(kryo).register(classOf[Row])
      verify(kryo).register(classOf[RowParseResult])
      verify(kryo).register(classOf[FrameRDD])
    }

  }
}
