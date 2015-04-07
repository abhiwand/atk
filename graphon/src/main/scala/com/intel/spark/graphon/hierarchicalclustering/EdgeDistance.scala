package com.intel.spark.graphon.hierarchicalclustering

import java.io.Serializable

/**
 * This is the edge distance class.
 */
object EdgeDistance extends Serializable {

  /**
   * Calculates the minimum distance of an edge list
   * @param edgeList a list of active edges
   * @return a tuple as follows (virtual vertex id, the min distance edge, the list of non-min distance edges)
   */
  def min(edgeList: Iterable[HierarchicalClusteringEdge]): (Long, HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge]) = {

    var dist: Float = Int.MaxValue
    var edgeWithMinDist: HierarchicalClusteringEdge = null
    var nonMinDistEdges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()

    if ((null != edgeList) && (!edgeList.isEmpty)) {
      for (edge <- edgeList) {
        if (null != edge) {
          if (edge.distance < dist) {

            // found a smaller distance edge.
            // save it in edgeWithMinDist & adjust the overall min distance
            dist = edge.distance
            if (edgeWithMinDist != null) {
              nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
            }
            edgeWithMinDist = edge
          }
          else if (edge.distance == dist) {
            if (edgeWithMinDist != null) {
              if (edge.src.toString < edgeWithMinDist.src.toString) {

                // found an equal distance edge but with node id smaller.
                // save it in edgeWithMinDist
                nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
                edgeWithMinDist = edge
              }
              else {

                // found equal distance edge but with node id higher. Add it to the list of non-selected
                nonMinDistEdges = nonMinDistEdges :+ edge
              }
            }
            else {

              // rare scenario. Found a small distance edge but edgeWithMinDist is not set.
              // set it.
              edgeWithMinDist = edge
            }
          }
          else {

            // found bigger distance edge. Add it to the list of non-selected.
            nonMinDistEdges = nonMinDistEdges :+ edge
          }
        }
      }

      if (null != edgeWithMinDist) {

        // edgeWithMinDist can be null in rar cases. We need to test for null
        if (edgeWithMinDist.dest.toString < edgeWithMinDist.src.toString) {

          // swap the node ids so the smaller node is always source
          val temp = edgeWithMinDist.src
          edgeWithMinDist.src = edgeWithMinDist.dest
          edgeWithMinDist.dest = temp
        }

        (edgeWithMinDist.src + edgeWithMinDist.dest, edgeWithMinDist, nonMinDistEdges)
      }
      else {
        (0, null, null)
      }
    }
    else {
      (0, null, null)
    }
  }

  /**
   * Sum (edgeDistance * SourceNodeWeight)
   *  -------------------------------------
   *  Sum (SourceNodeWeight)
   *
   * @param edges a list of active edges
   * @return the average distance, as per formula above
   */
  def weightedAvg(edges: Iterable[HierarchicalClusteringEdge]): Float = {
    var dist: Float = 0
    var nodeCount = 0

    for (e <- edges) {
      dist += (e.distance * e.srcNodeCount)
      nodeCount += e.srcNodeCount
    }

    if (nodeCount > 0) {
      dist = dist / nodeCount
    }

    dist
  }

  /**
   * Sum (edgeDistance)
   * -------------------
   * Total edges in the Iterable
   *
   * @param edges a list of active edges
   * @return the head of the input list with the distance adjusted as per formula
   */
  def simpleAvg(edges: Iterable[HierarchicalClusteringEdge]): HierarchicalClusteringEdge = {
    var dist: Float = 0
    var edgeCount = 0

    for (e <- edges) {
      dist += e.distance
      edgeCount += 1
    }

    if (edgeCount > 1) {
      edges.head.distance = dist / edgeCount
    }

    edges.head
  }

  // Same as simpleAvg + node swaps on the "head edge"
  def simpleAvgWithNodeSWap(edges: Iterable[HierarchicalClusteringEdge]): HierarchicalClusteringEdge = {
    var dist: Float = 0
    var edgeCount = 0

    for (e <- edges) {
      dist += e.distance
      edgeCount += 1
    }

    if (edgeCount > 1) {
      val head = edges.head

      head.distance = dist / edgeCount
      val tmpName = head.src
      val tmpNodeCount = head.srcNodeCount

      head.src = head.dest
      head.dest = tmpName
      head.srcNodeCount = head.destNodeCount
      head.destNodeCount = tmpNodeCount
    }

    edges.head
  }
}
