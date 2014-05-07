//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.graphbuilder.write.titan

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.intel.graphbuilder.elements.{ Property, Vertex }
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
