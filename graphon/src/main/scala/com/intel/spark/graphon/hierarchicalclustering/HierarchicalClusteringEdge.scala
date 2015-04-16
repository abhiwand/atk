package com.intel.spark.graphon.hierarchicalclustering

/**
 * This is the hierarchical clustering edge
 * @param src source node
 * @param srcNodeCount 1 if node is leaf, >1 if meta-node
 * @param dest destination node
 * @param destNodeCount 1 if node is leaf, >1 if meta-node
 * @param distance edge distance
 * @param isInternal true if the edge is internal (created through node edge collapse), false otherwise
 */
case class HierarchicalClusteringEdge(var src: Long,
                                      var srcNodeCount: Long,
                                      var dest: Long,
                                      var destNodeCount: Long,
                                      var distance: Float,
                                      isInternal: Boolean) {
  /**
   * Get the total node count of the edge
   * @return sum of src + dest counts
   */
  def getTotalNodeCount(): Long = {
    srcNodeCount + destNodeCount
  }

}
