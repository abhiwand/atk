package com.intel.spark.graphon.hierarchicalclustering

import org.apache.spark.rdd.RDD

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import org.apache.spark.rdd.RDD
import java.io.{ Serializable, FileWriter }
import org.apache.spark.SparkContext._

/**
 * This is the main clustering class.
 */
object HierarchicalClusteringMain extends Serializable {

  def execute(graph: RDD[HierarchicalClusteringEdge], titanConfig: SerializableBaseConfiguration): Unit = {

    var currentGraph: RDD[HierarchicalClusteringEdge] = graph
    val fileWriter = new FileWriter(HierarchicalClusteringConstants.OutputFilename, true)

    var iteration = 0
    val graphStorage = TitanStorage.connectToTitan(titanConfig)
    TitanStorage.addSchemaToTitan(graphStorage)
    TitanStorage.shutDown(graphStorage)

    while (currentGraph != null) {
      iteration = iteration + 1
      currentGraph = clusterNewLayer(currentGraph, fileWriter, iteration, titanConfig)
    }

    fileWriter.close()
  }

  private def clusterNewLayer(graph: RDD[HierarchicalClusteringEdge],
                              fileWriter: FileWriter,
                              iteration: Int, titanConfig: SerializableBaseConfiguration): RDD[HierarchicalClusteringEdge] = {

    // the list of edges to be collapsed and removed from the active graph
    val collapsableEdges = createCollapsableEdges(graph)
    collapsableEdges.cache()

    // the list of internal nodes connecting a newly created meta-node and the nodes of the collapsed edge
    val (internalEdges, nonSelectedEdges) = createInternalEdges(collapsableEdges, titanConfig)
    internalEdges.cache()
    nonSelectedEdges.cache()

    // the list of newly created active edges in the graph
    val activeEdges = createActiveEdges(nonSelectedEdges, internalEdges, fileWriter)
    activeEdges.cache()

    fileWriter.write("-------------Iteration " + iteration + " ---------------\n")

    val collapsableEdgesCount = collapsableEdges.count()
    if (collapsableEdges.count() > 0) {
      fileWriter.write("Collapsed edges " + collapsableEdgesCount + "\n")
    }
    else {
      fileWriter.write("No new collapsed edges\n")
      if (graph.count() > 0) {
        fileWriter.write("Current graph edges\n")
        graph.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
        fileWriter.write("current graph edges - done\n")
      }
    }
    fileWriter.flush()

    val internalEdgesCount = internalEdges.count()
    if (internalEdgesCount > 0) {
      fileWriter.write("Internal edges " + internalEdgesCount + "\n")
    }
    else {
      fileWriter.write("No new internal edges\n")
    }
    fileWriter.flush()

    val activeEdgesCount = activeEdges.count()
    if (activeEdges.count > 0) {
      internalEdges.unpersist()

      fileWriter.write("Active edges " + activeEdgesCount + "\n")
      fileWriter.flush()

      // create a key-value pair list of edges from the current graph (for subtractByKey)
      val currentGraphAsKVPair = graph.map((e: HierarchicalClusteringEdge) => (e.src, e))

      // create a key-value pair list of edges from the list of edges to be collapsed for subtractByKey)
      val collapsedEdgesAsKVPair = collapsableEdges.flatMap {
        case (collapsedEdge, nonSelectedEdges) => Seq((collapsedEdge.src, null),
          (collapsedEdge.dest, null))
      }

      //remove collapsed edges from the active graph - by src node
      val newGraphReducedBySrc = currentGraphAsKVPair.subtractByKey(collapsedEdgesAsKVPair).values

      //double the edges for edge selection algorithm
      val activeEdgesBothDirections = activeEdges.flatMap((e: HierarchicalClusteringEdge) => Seq(e, HierarchicalClusteringEdge(e.dest,
        e.destNodeCount,
        e.src,
        e.srcNodeCount,
        e.distance, e.isInternal))).distinct()
      activeEdges.unpersist()

      //remove collapsed edges from the active graph - by dest node
      val newGraphReducedBySrcAndDest = newGraphReducedBySrc.map((e: HierarchicalClusteringEdge) => (e.dest, e)).subtractByKey(collapsedEdgesAsKVPair).values
      val newGraphWithoutInternalEdges = activeEdgesBothDirections.union(newGraphReducedBySrcAndDest).coalesce(activeEdgesBothDirections.partitions.length, true)
      val distinctNewGraphWithoutInternalEdges = newGraphWithoutInternalEdges.filter(e => (e.src != e.dest))

      distinctNewGraphWithoutInternalEdges.cache()
      collapsableEdges.unpersist()

      fileWriter.write("Active edges to next iteration " + distinctNewGraphWithoutInternalEdges.count() + "\n")
      fileWriter.flush()

      distinctNewGraphWithoutInternalEdges
    }
    else {
      fileWriter.write("No new active edges - terminating...\n")
      fileWriter.flush()

      null
    }

  }

  /**
   * Create a set of edges to be added to the graph, replacing the collapsed ones
   * @param nonSelectedEdges - the set of collapsed edges
   * @param internalEdges - the set of internal edges (previously calculated from collapsed ones)
   * @return a list of new edges (containing meta-nodes) to be added to the active graph. The edge distance is updated/calculated for the new edges
   */
  private def createActiveEdges(nonSelectedEdges: RDD[HierarchicalClusteringEdge],
                                internalEdges: RDD[HierarchicalClusteringEdge], fileWriter: FileWriter): RDD[HierarchicalClusteringEdge] = {

    val activeEdges = nonSelectedEdges.map {
      case (e) => ((e.src, e.dest, e.destNodeCount), e)
    }.groupByKey()

    // create new active edges
    val activeEdgesWithWeightedAvgDistance = activeEdges.map {
      case ((srcNode, destNode, destNodeCount), newEdges) =>
        val tempEdgeForMetaNode = newEdges.head

        HierarchicalClusteringEdge(tempEdgeForMetaNode.src,
          tempEdgeForMetaNode.srcNodeCount,
          destNode,
          destNodeCount,
          EdgeDistance.weightedAvg(newEdges), false)
    }.distinct()

    val newEdges = (internalEdges union activeEdgesWithWeightedAvgDistance).coalesce(internalEdges.partitions.length, true).map(
      (e: HierarchicalClusteringEdge) => (e.dest, e)
    ).groupByKey()

    // update the dest node with meta-node in the list
    val newEdgesWithMetaNodeForDest = newEdges.map {
      case (dest, newEdges) => EdgeManager.replaceWithMetaNode(newEdges)
    }.flatMap(identity)

    val newEdgesWithMetaNodeGrouped = newEdgesWithMetaNodeForDest.map(
      (e: HierarchicalClusteringEdge) => ((e.src, e.dest), e)
    ).groupByKey()

    // recalculate the edge distance if several outgoing edges go into the same meta-node
    val newEdgesWithMetaNodesAndDistUpdated = newEdgesWithMetaNodeGrouped.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvgWithNodeSWap(edges)
    }.map {
      (e: HierarchicalClusteringEdge) => ((e.src, e.dest), e)
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
  private def createInternalEdges(collapsedEdges: RDD[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])],
                                  titanConfig: SerializableBaseConfiguration): (RDD[HierarchicalClusteringEdge], RDD[HierarchicalClusteringEdge]) = {

    val internalEdges = collapsedEdges.mapPartitions {
      case edges: Iterator[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])] => {
        val titanConnector = TitanGraphConnector(titanConfig)
        val graph = titanConnector.connect()
        val tmp = edges.map {
          case edge =>
            val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(edge._1, graph)
            val replacedEdges = EdgeManager.createActiveEdgesForMetaNode(metanode, metanodeCount, edge._2).map(_._2)
            (metaEdges, replacedEdges)
        }.toList

        graph.commit()
        graph.shutdown()

        tmp.toIterator
      }
    }
    internalEdges.cache()
    (internalEdges.flatMap(_._1), internalEdges.flatMap(_._2))
  }

  /**
   * Create collapsed edges for the current graph
   * @param graph the active graph at ith iteration
   * @return a list of edges to be collapsed at this iteration
   */
  private def createCollapsableEdges(graph: RDD[HierarchicalClusteringEdge]): RDD[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])] = {

    val collapsableEdges = graph.map((e: HierarchicalClusteringEdge) => (e.src, e)).groupByKey().map {
      case (sourceNode, allEdges) =>
        val min = EdgeDistance.min(allEdges)
        min match {
          case (vertexId,
            minEdge,
            nonSelectedEdges) => (vertexId, (minEdge, nonSelectedEdges))
        }
    }.groupByKey().filter {
      case (vertexId, pairedEdgeList) => EdgeManager.canEdgeCollapse(pairedEdgeList)
    }

    collapsableEdges.map {
      case (vertexId, pairedEdgeList) =>
        EdgeManager.createOutgoingEdgesForMetaNode(pairedEdgeList)
    }.filter {
      case (collapsableEdge, outgoingEdgeList) => (collapsableEdge != null)
    }
  }
}
