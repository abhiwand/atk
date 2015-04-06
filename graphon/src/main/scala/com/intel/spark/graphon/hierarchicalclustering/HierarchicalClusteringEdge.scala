package com.intel.spark.graphon.hierarchicalclustering

case class HierarchicalClusteringEdge(var src: Long,
                                      var srcNodeCount: Int,
                                      var dest: Long,
                                      var destNodeCount: Int,
                                      var distance: Float,
                                      isInternal: Boolean) {
  def getTotalNodeCount(): Int = {
    srcNodeCount + destNodeCount
  }

}
