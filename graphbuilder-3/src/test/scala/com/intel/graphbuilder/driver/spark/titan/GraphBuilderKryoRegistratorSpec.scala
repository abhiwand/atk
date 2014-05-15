package com.intel.graphbuilder.driver.spark.titan

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import com.esotericsoftware.kryo.Kryo
import com.intel.graphbuilder.parser.rule.{ConstantValue, Value, ParsedValue, EdgeRule}
import com.intel.graphbuilder.elements.{GbIdToPhysicalId, Property, Vertex, Edge}
import com.intel.graphbuilder.parser.ColumnDef

class GraphBuilderKryoRegistratorSpec extends Specification with Mockito {

  "GraphBuilderKryoRegistrator" should {

    "register expected classes" in {

      val kryo = mock[Kryo]

      // invoke method under test
      new GraphBuilderKryoRegistrator().registerClasses(kryo)

      // make sure all of the classes that will be serialized many times are included
      there was one (kryo).register(classOf[Edge])
      there was one (kryo).register(classOf[Vertex])
      there was one (kryo).register(classOf[Property])
      there was one (kryo).register(classOf[Value])
      there was one (kryo).register(classOf[ParsedValue])
      there was one (kryo).register(classOf[ConstantValue])
      there was one (kryo).register(classOf[GbIdToPhysicalId])
      
      // check some other ones too
      there was one (kryo).register(classOf[EdgeRule])
      there was one (kryo).register(classOf[ColumnDef])
    }
    
  }
}
