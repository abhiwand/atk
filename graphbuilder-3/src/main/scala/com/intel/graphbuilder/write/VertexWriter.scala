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
