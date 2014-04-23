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

package com.intel.graphbuilder.write

import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.write.dao.VertexDAO
import com.tinkerpop.blueprints


/**
 * Write vertices to a Blueprints compatible Graph database
 *
 * @param vertexDAO data access for creating and updating Vertices in the Graph database
 * @param append True to append to an existing Graph database
 */
class VertexWriter(vertexDAO: VertexDAO, append: Boolean) extends Serializable {

  /**
   * Write a vertex to the Graph database
   */
  def write(vertex: Vertex): blueprints.Vertex = {
    if (append) {
      vertexDAO.updateOrCreate(vertex)
    }
    else {
      vertexDAO.create(vertex)
    }
  }

}
