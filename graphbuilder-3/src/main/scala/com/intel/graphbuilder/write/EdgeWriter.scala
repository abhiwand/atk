package com.intel.graphbuilder.write

import com.intel.graphbuilder.elements.Edge
import com.intel.graphbuilder.write.dao.EdgeDAO
import com.tinkerpop.blueprints


/**
 * Write parsed Edges into a Blueprints compatible Graph database
 *
 * @param edgeDAO data access creating and updating Edges in the Graph database
 * @param append True to append to an existing graph
 */
class EdgeWriter(edgeDAO: EdgeDAO, append: Boolean) extends Serializable {

  def write(edge: Edge): blueprints.Edge = {
    if (append) {
      edgeDAO.updateOrCreate(edge)
    }
    else {
      edgeDAO.create(edge)
    }
  }

}
