package com.intel.graphbuilder.write

import org.specs2.mutable.Specification
import com.intel.graphbuilder.write.dao.EdgeDAO
import com.intel.graphbuilder.elements.Edge
import org.specs2.mock.Mockito

class EdgeWriterSpec extends Specification with Mockito {

  "EdgeWriter" should {

    "support append true" in {
      // setup mocks
      val edgeDAO = mock[EdgeDAO]
      val edge = mock[Edge]

      // instantiated class under test
      val edgeWriter = new EdgeWriter(edgeDAO, append = true)

      // invoke method under test
      edgeWriter.write(edge)

      // validate
      there was one(edgeDAO).updateOrCreate(edge)
    }

    "support append false" in {
      // setup mocks
      val edgeDAO = mock[EdgeDAO]
      val edge = mock[Edge]

      // instantiate class under test
      val edgeWriter = new EdgeWriter(edgeDAO, append = false)

      // invoke method under test
      edgeWriter.write(edge)

      // validate
      there was one(edgeDAO).create(edge)
    }
  }

}
