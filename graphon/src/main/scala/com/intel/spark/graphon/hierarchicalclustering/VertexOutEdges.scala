package com.intel.spark.graphon.hierarchicalclustering

/**
 * List of Edges from a source Vertex
 * @param minDistanceEdge - the minimum distance edge associated with the vertex
 * @param higherDistanceEdgeList - the remainder of the vertex edges (the non-minimum diatance)
 */
case class VertexOutEdges(minDistanceEdge: HierarchicalClusteringEdge,
                          higherDistanceEdgeList: Iterable[HierarchicalClusteringEdge]) {

  def sourceVId: Long = minDistanceEdge.src
}

