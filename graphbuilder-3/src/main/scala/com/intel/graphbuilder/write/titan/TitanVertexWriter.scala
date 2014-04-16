package com.intel.graphbuilder.write.titan

import com.intel.graphbuilder.elements.{GbIdToPhysicalId, Vertex}
import com.intel.graphbuilder.write.VertexWriter
import com.intel.graphbuilder.write.titan.TitanIdUtils.titanId

/**
 * Wraps a blueprints VertexWriter to add some Titan specific functionality that is needed
 *
 * @param vertexWriter blueprints VertexWriter
 */
class TitanVertexWriter(vertexWriter: VertexWriter) extends Serializable {

  /**
   * Write a vertex returning the GbIdToPhysicalId mapping so that we can match up Physical ids to Edges
   */
  def write(vertex: Vertex): GbIdToPhysicalId = {
    val bpVertex = vertexWriter.write(vertex)
    new GbIdToPhysicalId(vertex.gbId, titanId(bpVertex).asInstanceOf[AnyRef])
  }
}

