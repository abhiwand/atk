package com.intel.graphbuilder.write

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.intel.graphbuilder.write.dao.VertexDAO
import com.intel.graphbuilder.elements.Vertex

class VertexWriterSpec extends Specification with Mockito {


  "VertexWriter" should {

    "support append true" in {
      // setup mocks
      val vertexDAO = mock[VertexDAO]
      val vertex = mock[Vertex]

      // instantiated class under test
      val vertexWriter = new VertexWriter(vertexDAO, append = true)

      // invoke method under test
      vertexWriter.write(vertex)

      // validate
      there was one(vertexDAO).updateOrCreate(vertex)
    }

    "support append false" in {
      // setup mocks
      val vertexDAO = mock[VertexDAO]
      val vertex = mock[Vertex]

      // instantiate class under test
      val vertexWriter = new VertexWriter(vertexDAO, append = false)

      // invoke method under test
      vertexWriter.write(vertex)

      // validate
      there was one(vertexDAO).create(vertex)
    }
  }

}
