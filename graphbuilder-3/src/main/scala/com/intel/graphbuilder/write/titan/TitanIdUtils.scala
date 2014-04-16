package com.intel.graphbuilder.write.titan

import com.thinkaurelius.titan.core.TitanElement
import com.tinkerpop.blueprints


/**
 * Gets the Titan assigned physical ID from Blueprints
 */
object TitanIdUtils {

  /**
   * Obtain the Titan-assigned ID from a Blueprints Element (Vertex or Edge)
   * @param blueprintsElement  a Titan implementation of a Blueprints Element (Vertex or Edge)
   * @return its Titan-assigned ID.
   */
  def titanId(blueprintsElement: blueprints.Element): Long = {
    blueprintsElement.asInstanceOf[TitanElement].getID
  }
}
