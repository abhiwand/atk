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

package com.intel.graphbuilder.write

import org.mockito.Mockito._
import org.scalatest.{ Matchers, WordSpec }
import com.intel.graphbuilder.write.dao.VertexDAO
import com.intel.graphbuilder.elements.GBVertex
import org.scalatest.mock.MockitoSugar

class VertexWriterTest extends WordSpec with Matchers with MockitoSugar {

  "VertexWriter" should {

    "support append true" in {
      // setup mocks
      val vertexDAO = mock[VertexDAO]
      val vertex = mock[GBVertex]

      // instantiated class under test
      val vertexWriter = new VertexWriter(vertexDAO, append = true)

      // invoke method under test
      vertexWriter.write(vertex)

      // validate
      verify(vertexDAO).updateOrCreate(vertex)
    }

    "support append false" in {
      // setup mocks
      val vertexDAO = mock[VertexDAO]
      val vertex = mock[GBVertex]

      // instantiate class under test
      val vertexWriter = new VertexWriter(vertexDAO, append = false)

      // invoke method under test
      vertexWriter.write(vertex)

      // validate
      verify(vertexDAO).create(vertex)
    }
  }

}
