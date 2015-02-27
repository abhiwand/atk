//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import com.intel.graphbuilder.elements.{ Property, GBVertex }
import com.intel.graphbuilder.write.VertexWriter
import com.thinkaurelius.titan.core.TitanVertex

class TitanVertexWriterTest extends WordSpec with Matchers with MockitoSugar {

  "TitanVertexWriter" should {

    "use the underlying writer and populate the gbIdToPhysicalId mapping" in {
      // setup mocks
      val vertexWriter = mock[VertexWriter]
      val gbVertex = mock[GBVertex]
      val titanVertex = mock[TitanVertex]
      val gbId = mock[Property]
      val physicalId = new java.lang.Long(123)

      when(gbVertex.gbId).thenReturn(gbId)
      when(titanVertex.getLongId()).thenReturn(physicalId)
      when(vertexWriter.write(gbVertex)).thenReturn(titanVertex)

      // instantiated class under test
      val titanVertexWriter = new TitanVertexWriter(vertexWriter)

      // invoke method under test
      val gbIdToPhysicalId = titanVertexWriter.write(gbVertex)

      // validate
      verify(vertexWriter).write(gbVertex)
      gbIdToPhysicalId.gbId shouldBe gbId
      gbIdToPhysicalId.physicalId shouldBe physicalId
    }

  }

}
