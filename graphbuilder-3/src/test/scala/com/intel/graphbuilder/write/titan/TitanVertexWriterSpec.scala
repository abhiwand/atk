package com.intel.graphbuilder.write.titan

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.intel.graphbuilder.write.dao.VertexDAO
import com.intel.graphbuilder.elements.{Property, Vertex}
import com.intel.graphbuilder.write.dao.VertexDAO
import com.intel.graphbuilder.write.VertexWriter
import com.thinkaurelius.titan.core.TitanVertex


class TitanVertexWriterSpec extends Specification with Mockito {


  "TitanVertexWriter" should {

    "use the underlying writer and populate the gbIdToPhysicalId mapping" in {
      // setup mocks
      val vertexWriter = mock[VertexWriter]
      val gbVertex = mock[Vertex]
      val titanVertex = mock[TitanVertex]
      val gbId = mock[Property]
      val physicalId = new java.lang.Long(123)

      gbVertex.gbId.returns(gbId)
      titanVertex.getID.returns(physicalId)
      vertexWriter.write(gbVertex).returns(titanVertex)

      // instantiated class under test
      val titanVertexWriter = new TitanVertexWriter(vertexWriter)

      // invoke method under test
      val gbIdToPhysicalId = titanVertexWriter.write(gbVertex)

      // validate
      there was one(vertexWriter).write(gbVertex)
      gbIdToPhysicalId.gbId mustEqual gbId
      gbIdToPhysicalId.physicalId mustEqual physicalId
    }

  }

}
