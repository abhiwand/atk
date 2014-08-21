package com.intel.graphbuilder.driver.spark.titan

import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._
import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.parser.rule.{ ConstantValue, Value, ParsedValue, EdgeRule }
import com.intel.graphbuilder.elements.{ GbIdToPhysicalId, Property, Vertex, Edge }
import com.intel.graphbuilder.parser.ColumnDef
import org.scalatest.mock.MockitoSugar

class GraphBuilderKryoRegistratorTest extends WordSpec with Matchers with MockitoSugar {

  "GraphBuilderKryoRegistrator" should {

    "register expected classes" in {

      val kryo = mock[Kryo]

      // invoke method under test
      new GraphBuilderKryoRegistrator().registerClasses(kryo)

      // make sure all of the classes that will be serialized many times are included
      verify(kryo).register(classOf[Edge])
      verify(kryo).register(classOf[Vertex])
      verify(kryo).register(classOf[Property])
      verify(kryo).register(classOf[Value])
      verify(kryo).register(classOf[ParsedValue])
      verify(kryo).register(classOf[ConstantValue])
      verify(kryo).register(classOf[GbIdToPhysicalId])

      // check some other ones too
      verify(kryo).register(classOf[EdgeRule])
      verify(kryo).register(classOf[ColumnDef])
    }

  }
}
