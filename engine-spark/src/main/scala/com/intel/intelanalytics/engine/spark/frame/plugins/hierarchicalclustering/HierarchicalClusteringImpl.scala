package com.intel.intelanalytics.engine.spark.frame.plugins.hierarchicalclustering

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain.frame.FrameReference
import org.apache.spark.frame.FrameRDD
import org.apache.spark.rdd.RDD
import java.io.{ Serializable, FileWriter }
import scala.io.Source
import org.apache.spark.SparkContext._

object HierarchicalClusteringImpl extends Serializable {
  def execute(frame: FrameRDD) = {
    val graph = IOManager.loadInitialGraph(frame)
    HierarchicalGraphClustering.execute(graph)
  }
}

/**
 * This is the IO manager class. It provides the following IO capabilities:
 *   1. read initial graph from storage (database)
 *   2. add edges to database
 *   3. add vertices to database
 *   4. request node id for meta-nodes
 */
object IOManager extends Serializable {
  def getNodeName(edge: HCEdge): String = {
    edge.src + "_" + edge.dest
  }

  def loadInitialGraph(graph: FrameRDD): RDD[HCEdge] = {
    //    val graphEdges = sc.parallelize(graph)

    val f = graph.collect()
    val initialRdd = graph.mapRows(row => row.valuesAsArray())
    val reversedRdd = initialRdd.map(row => Array(row(1), row(0), row(2)))
    val unionRdd = initialRdd ++ reversedRdd
    val z = unionRdd.collect()
    //    val x = unionRdd.map(row => Edge(row(0).toString, 1, row(1).toString, 1, row(2).asInstanceOf[Float], false))
    val x = unionRdd.map(row => HCEdge.edgeFactory("f", 1, "a", 1, 2.0f, false))
    val t = x.collect()
    x
  }

}

/**
 * This is the edge distance class.
 */
object EdgeDistance extends Serializable {
  def min(edgeList: Iterable[HCEdge]): (String, HCEdge, Iterable[HCEdge]) = {

    var dist: Float = Int.MaxValue
    var edgeWithMinDist: HCEdge = null
    var nonMinDistEdges: List[HCEdge] = List[HCEdge]()

    if ((null != edgeList) && (!edgeList.isEmpty)) {
      for (edge <- edgeList) {
        if (null != edge) {
          if (edge.distance < dist) {
            //
            // found a smaller distance edge.
            // save it in edgeWithMinDist & adjust the overall min distance
            //
            dist = edge.distance
            if (edgeWithMinDist != null) {
              nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
            }
            edgeWithMinDist = edge
          }
          else if (edge.distance == dist) {
            if (edgeWithMinDist != null) {
              if (edge.src < edgeWithMinDist.src) {
                //
                // found an equal distance edge but with node id smaller.
                // save it in edgeWithMinDist
                //
                nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
                edgeWithMinDist = edge
              }
              else {
                //
                // found equal distance edge but with node id higher. Add it to the list of non-selected
                //
                nonMinDistEdges = nonMinDistEdges :+ edge
              }
            }
            else {
              //
              // rare scenario. Found a small distance edge but edgeWithMinDist is not set.
              // set it.
              //
              edgeWithMinDist = edge
            }
          }
          else {
            //
            // found bigger distance edge. Add it to the list of non-selected.
            //
            nonMinDistEdges = nonMinDistEdges :+ edge
          }
        }
      }

      if (null != edgeWithMinDist) {
        //
        // edgeWithMinDist can be null in rar cases. We need to test for null
        //
        if (edgeWithMinDist.dest < edgeWithMinDist.src) {
          //
          // swap the node ids so the smaller node is always source
          //
          val temp = edgeWithMinDist.src
          edgeWithMinDist.src = edgeWithMinDist.dest
          edgeWithMinDist.dest = temp
        }

        (edgeWithMinDist.src + edgeWithMinDist.dest, edgeWithMinDist, nonMinDistEdges)
      }
      else {
        (null, null, null)
      }
    }
    else {
      (null, null, null)
    }
  }

  //
  // Sum (edgeDistance * SourceNodeWeight)
  // -------------------------------------
  // Sum (SourceNodeWeight)
  //
  def weightedAvg(edges: Iterable[HCEdge]): Float = {
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

  //
  // Sum (edgeDistance)
  // -------------------
  // Total edges in the Iterable
  //
  def simpleAvg(edges: Iterable[HCEdge]): HCEdge = {
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

  //
  // Same as simpleAvg + node swaps on the "head edge"
  //
  def simpleAvgWithNodeSWap(edges: Iterable[HCEdge]): HCEdge = {
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

/**
 * This is the edge manager class.
 */
object EdgeManager extends Serializable {
  /**
   *
   * @param list a  list of iterable edges. If the list has 2 elements, the head element (an edge) of any of the lists can collapse
   * @return true if the edge can collapse; false otherwise
   */
  def canEdgeCollapse(list: Iterable[(HCEdge, Iterable[HCEdge])]): Boolean = {
    (null != list) && (list.toArray.length > 1)
  }

  /**
   * Replace a node with associated meta-node in edges.
   * @param edgeList - a list with the following property:
   *                 head - an edge with the meta node as source node
   *                 tail - a list of edges whose destination nodes will need to be replaced
   * @return the edge list (less the head element) with the destination node replaced by head.source
   */
  def replaceWithMetaNode(edgeList: Iterable[HCEdge]): Iterable[HCEdge] = {
    var edges = edgeList
    if (edges.toArray.length > 1) {
      val head: HCEdge = edges.head
      for (edge <- edgeList) {
        if (edge != head) {
          edge.distance = edge.distance * edge.destNodeCount
          edge.dest = head.src
          edge.destNodeCount = head.srcNodeCount
        }
      }
      edges = edges.drop(1)
    }
    else {
      edges = edges.filter(e => e.isInternal == false)
    }

    edges
  }

  /**
   * Creates a flat list of edges (to be interpreted as outgoing edges) for a meta-node
   * @param list a list of (lists of) edges. The source node of the head element of each list is the metanode
   * @return a flat list of outgoing edges for metanode
   */
  def createOutgoingEdgesForMetaNode(list: Iterable[(HCEdge, Iterable[HCEdge])]): (HCEdge, Iterable[HCEdge]) = {
    var outgoingEdges: List[HCEdge] = List[HCEdge]()
    var edge: HCEdge = null

    if ((null != list) && (!list.isEmpty)) {
      for (edgeList <- list) {
        if ((null != edgeList) && (null != edgeList._2)) {
          outgoingEdges :::= edgeList._2.toList
          edge = edgeList._1
        }
      }
    }
    (edge, outgoingEdges)
  }

  /**
   * Creates 2 internal edges for a collapsed edge
   * @param edge a collapsed edge
   * @return 2 internal edges replacing the collapsed edge in the graph
   */
  def createInternalEdgesForMetaNode(edge: HCEdge): Iterable[HCEdge] = {
    var edges: List[HCEdge] = List[HCEdge]()

    if (null != edge) {
      val parent = IOManager.getNodeName(edge)
      edges = edges :+ HCEdge(parent,
        edge.getTotalNodeCount,
        edge.src,
        edge.srcNodeCount,
        1, true)
      edges = edges :+ HCEdge(parent,
        edge.getTotalNodeCount,
        edge.dest,
        edge.destNodeCount,
        1, true)
    }

    edges
  }

  /**
   * Creates a list of active edges for meta-node
   * @param e - the edge containing the meta-node parameters as dest
   * @param collapsableEdges - the list of collapsed edges for meta-node
   * @return - a new list of active graph edges connecting meta-nodes
   */
  def createActiveEdgesForMetaNode(e: HCEdge, collapsableEdges: Iterable[HCEdge]): Iterable[((String, Int), HCEdge)] = {
    val defaultActiveEdges = List[((String, Int), HCEdge)]()

    if (null != e) {
      collapsableEdges.map(edge => ((edge.dest, edge.destNodeCount),
        HCEdge(IOManager.getNodeName(e),
          e.getTotalNodeCount,
          edge.dest,
          edge.destNodeCount,
          edge.distance, false)))
    }
    else {
      defaultActiveEdges
    }
  }
}

/**
 * This is the main clustering class.
 */
object HierarchicalGraphClustering extends Serializable {

  def execute(graph: RDD[HCEdge]): Unit = {

    var currentGraph: RDD[HCEdge] = graph
    val fileWriter = new FileWriter("test.txt", true)

    fileWriter.write("Initial graph\n")
    val data = graph.collect()
    data.foreach((e: HCEdge) => fileWriter.write(e.toString() + "\n"))
    fileWriter.write("\n\n")
    fileWriter.flush()

    var iteration = 0
    while (currentGraph != null) {
      iteration = iteration + 1
      currentGraph = clusterNewLayer(currentGraph, fileWriter, iteration)
    }

    fileWriter.close()
  }

  private def clusterNewLayer(graph: RDD[HCEdge],
                              fileWriter: FileWriter,
                              iteration: Int): RDD[HCEdge] = {
    // the list of edges to be collapsed and removed from the active graph
    val collapsableEdges = createCollapsableEdges(graph)

    // the list of internal nodes connecting a newly created meta-node and the nodes of the collapsed edge
    val internalEdges = createInternalEdges(collapsableEdges)

    // the list of newly created active edges in the graph
    val activeEdges = createActiveEdges(collapsableEdges, internalEdges)

    fileWriter.write("-------------Iteration " + iteration + " ---------------\n")

    if (collapsableEdges.count() > 0) {
      fileWriter.write("Collapsed edges - start\n")
      collapsableEdges.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("Collapsed edges - done\n\n")
    }
    else {
      fileWriter.write("No new collapsed edges\n")
    }
    fileWriter.flush()

    if (internalEdges.count() > 0) {
      fileWriter.write("Internal edges - start\n")
      internalEdges.collect().foreach((e: HCEdge) => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("Internal edges - done\n\n")
    }
    else {
      fileWriter.write("No new internal edges\n")
    }
    fileWriter.flush()

    if (activeEdges.count > 0) {

      //double the edges for edge selection algorithm
      val activeEdgesBothDirections = activeEdges.flatMap((e: HCEdge) => Seq(e, HCEdge(e.dest,
        e.destNodeCount,
        e.src,
        e.srcNodeCount,
        e.distance, e.isInternal)))

      // create a key-value pair list of edges from the current graph (for subtractByKey)
      val currentGraphAsKVPair = graph.map((e: HCEdge) => (e.src, e))

      // create a key-value pair list of edges from the list of edges to be collapsed for subtractByKey)
      val collapsedEdgesAsKVPair = collapsableEdges.flatMap {
        case (collapsedEdge, nonSelectedEdges) => Seq((collapsedEdge.src, collapsedEdge),
          (collapsedEdge.dest, collapsedEdge))
      }

      //remove collapsed edges from the active graph - by src node
      val newGraphReducedBySrc = currentGraphAsKVPair.subtractByKey(collapsedEdgesAsKVPair).map {
        case (nodeName, edge) => edge
      }

      //remove collapsed edges from the active graph - by dest node
      val newGraphReducedBySrcAndDest = newGraphReducedBySrc.map((e: HCEdge) => (e.dest, e)).subtractByKey(collapsedEdgesAsKVPair).map {
        case (nodeName, edge) => edge
      }
      val newGraphWithoutInternalEdges = (activeEdgesBothDirections union newGraphReducedBySrcAndDest).coalesce(activeEdgesBothDirections.partitions.length, true)

      fileWriter.write("Active new edges - start\n")
      newGraphWithoutInternalEdges.collect().foreach((e: HCEdge) => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("Active new edges - done\n\n")
      fileWriter.flush()

      newGraphWithoutInternalEdges
    }
    else {
      fileWriter.write("No new active edges - terminating...\n")
      fileWriter.flush()

      null
    }
  }

  /**
   * Create a set of edges to be added to the graph, replacing the collapsed ones
   * @param collapsedEdges - the set of collapsed edges
   * @param internalEdges - the set of internal edges (previously calculated from collapsed ones)
   * @return a list of new edges (containing meta-nodes) to be added to the active graph. The edge distance is updated/calculated for the new edges
   */
  private def createActiveEdges(collapsedEdges: RDD[(HCEdge, Iterable[HCEdge])],
                                internalEdges: RDD[HCEdge]): RDD[HCEdge] = {
    val edgeManager = EdgeManager

    val activeEdges = collapsedEdges.map {
      case (minEdge, nonSelectedEdges) => edgeManager.createActiveEdgesForMetaNode(minEdge, nonSelectedEdges)
    }.flatMap(identity).groupByKey()

    // create new active edges
    val activeEdgesWithWeightedAvgDistance = activeEdges.map {
      case ((destNode, destNodeCount), newEdges) =>
        val tempEdgeForMetaNode = newEdges.head

        HCEdge(tempEdgeForMetaNode.src,
          tempEdgeForMetaNode.srcNodeCount,
          destNode,
          destNodeCount,
          EdgeDistance.weightedAvg(newEdges), false)
    }
    val newEdges = (internalEdges union activeEdgesWithWeightedAvgDistance).coalesce(internalEdges.partitions.length, true).map(
      (e: HCEdge) => (e.dest, e)
    ).groupByKey()

    // update the dest node with meta-node in the list
    val newEdgesWithMetaNodeForDest = newEdges.map {
      case (dest, newEdges) => edgeManager.replaceWithMetaNode(newEdges)
    }.flatMap(identity)

    val newEdgesWithMetaNodeGrouped = newEdgesWithMetaNodeForDest.map(
      (e: HCEdge) => ((e.src, e.dest), e)
    ).groupByKey()

    // recalculate the edge distance if several outgoing edges go into the same meta-node
    val newEdgesWithMetaNodesAndDistUpdated = newEdgesWithMetaNodeGrouped.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvgWithNodeSWap(edges)
    }.map {
      (e: HCEdge) => ((e.src, e.dest), e)
    }.groupByKey()

    newEdgesWithMetaNodesAndDistUpdated.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvg(edges)
    }
  }

  /**
   * Create internal edges for all collapsed edges of the graph
   * @param collapsedEdges - a list of collapsed edges
   * @return an RDD of newly created internal edges
   */
  private def createInternalEdges(collapsedEdges: RDD[(HCEdge, Iterable[HCEdge])]): RDD[HCEdge] = {
    val edgeManager = EdgeManager

    collapsedEdges.map {
      case (edge, edgeList) => edgeManager.createInternalEdgesForMetaNode(edge)
    }.flatMap(identity)
  }

  /**
   * Create collapsed edges for the current graph
   * @param graph the active graph at ith iteration
   * @return a list of edges to be collapsed at this iteration
   */
  private def createCollapsableEdges(graph: RDD[HCEdge]): RDD[(HCEdge, Iterable[HCEdge])] = {
    val edgeManager = EdgeManager

    val collapsableEdges = graph.map((e: HCEdge) => (e.src, e)).groupByKey().map {
      case (sourceNode, allEdges) =>
        val min = EdgeDistance.min(allEdges)
        min match {
          case (vertexName,
            minEdge,
            nonSelectedEdges) => (vertexName, (minEdge, nonSelectedEdges))
        }
    }.groupByKey().filter {
      case (vertexName, pairedEdgeList) => edgeManager.canEdgeCollapse(pairedEdgeList)
    }

    collapsableEdges.map {
      case (vertexName, pairedEdgeList) =>
        val outgoingEdges = edgeManager.createOutgoingEdgesForMetaNode(pairedEdgeList)
        outgoingEdges match {
          case (collapsableEdge, outgoingEdgeList) => (collapsableEdge, outgoingEdgeList)
        }
    }
  }
}
