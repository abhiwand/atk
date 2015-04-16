package com.intel.spark.graphon.hierarchicalclustering

import com.intel.event.EventLogging
import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.spark.graphon.hierarchicalclustering.HierarchicalClusteringStorage
import org.apache.spark.rdd.RDD

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import org.apache.spark.rdd.RDD
import java.io.{ Serializable }
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * This is the main clustering class.
 */
object HierarchicalClusteringFunctions extends Serializable with EventLogging {

  /**
   * Convert the storage graph into a hierarchical edge RDD
   * @param vertices the list of vertices for the initial graph
   * @param edges the list of edges for the initial graph
   * @param dbConnectionConfig serializable configuration file
   */
  def execute(vertices: RDD[GBVertex], edges: RDD[GBEdge], dbConnectionConfig: SerializableBaseConfiguration): Unit = {

    val graphAdRdd: RDD[HierarchicalClusteringEdge] = edges.map {
      case e =>
        val edgeDistProperty = e.getProperty(HierarchicalClusteringConstants.EdgeDistanceProperty)
          .getOrElse(throw new Exception(s"Edge does not have ${HierarchicalClusteringConstants.EdgeDistanceProperty} property"))
        HierarchicalClusteringEdge(e.headPhysicalId.asInstanceOf[Number].longValue,
          HierarchicalClusteringConstants.DefaultNodeCount,
          e.tailPhysicalId.asInstanceOf[Number].longValue,
          HierarchicalClusteringConstants.DefaultNodeCount,
          1 - edgeDistProperty.value.asInstanceOf[Float], false)
    }.distinct()

    mainLoop(graphAdRdd, dbConnectionConfig)
  }

  /**
   * This is the main loop of the algorithm
   * @param graph initial in memory graph as RDD of hierarchical clustering edges
   * @param dbConnectionConfig the config for storage
   */
  def mainLoop(graph: RDD[HierarchicalClusteringEdge], dbConnectionConfig: SerializableBaseConfiguration): Unit = {

    var currentGraph: RDD[HierarchicalClusteringEdge] = graph
    var iteration = 0

    configStorage(dbConnectionConfig)
    while (currentGraph != null) {
      iteration = iteration + 1
      currentGraph = clusterNewLayer(currentGraph, iteration, dbConnectionConfig)
    }
  }

  /**
   * Add schema to the storage configuration
   * @param dbConnectionConfig the initial/default configuration
   */
  private def configStorage(dbConnectionConfig: SerializableBaseConfiguration): Unit = {

    val hcStorage = HierarchicalClusteringStorage(dbConnectionConfig)
    hcStorage.addSchema()
    hcStorage.shutdown()
  }

  /**
   * Creates a set of meta-node and a set of internal nodes and edges (saved to storage)
   * @param graph (n-1) in memory graph (as an RDD of hierarchical clustering edges)
   * @param iteration current iteration, testing purposes only
   * @param dbConnectionConfig storage configuration file
   * @return (n) in memory graph (as an RDD of hierarchical clustering edges)
   */
  private def clusterNewLayer(graph: RDD[HierarchicalClusteringEdge],
                              iteration: Int,
                              dbConnectionConfig: SerializableBaseConfiguration): RDD[HierarchicalClusteringEdge] = {

    // the list of edges to be collapsed and removed from the active graph
    val collapsableEdges = createCollapsableEdges(graph)
    collapsableEdges.persist(StorageLevel.MEMORY_AND_DISK)

    // the list of internal nodes connecting a newly created meta-node and the nodes of the collapsed edge
    val (internalEdges, nonSelectedEdges) = createInternalEdges(collapsableEdges, dbConnectionConfig)
    internalEdges.persist(StorageLevel.MEMORY_AND_DISK)
    nonSelectedEdges.persist(StorageLevel.MEMORY_AND_DISK)

    // the list of newly created active edges in the graph
    val activeEdges = createActiveEdges(nonSelectedEdges, internalEdges)
    activeEdges.persist(StorageLevel.MEMORY_AND_DISK)

    info("-------------Iteration " + iteration + " ---------------")

    val collapsableEdgesCount = collapsableEdges.count()
    if (collapsableEdges.count() > 0) {
      info("Collapsed edges " + collapsableEdgesCount)
    }
    else {
      info("No new collapsed edges")
    }

    val internalEdgesCount = internalEdges.count()
    if (internalEdgesCount > 0) {
      info("Internal edges " + internalEdgesCount)
    }
    else {
      info("No new internal edges")
    }

    val activeEdgesCount = activeEdges.count()
    if (activeEdges.count > 0) {
      internalEdges.unpersist()

      info("Active edges " + activeEdgesCount)

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

      distinctNewGraphWithoutInternalEdges.persist(StorageLevel.MEMORY_AND_DISK)
      collapsableEdges.unpersist()

      info("Active edges to next iteration " + distinctNewGraphWithoutInternalEdges.count())

      distinctNewGraphWithoutInternalEdges
    }
    else {
      info("No new active edges - terminating...")
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
                                internalEdges: RDD[HierarchicalClusteringEdge]): RDD[HierarchicalClusteringEdge] = {

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
      case ((src, dest), edges) => EdgeDistance.simpleAvg(edges, true)
    }.map {
      (e: HierarchicalClusteringEdge) => ((e.src, e.dest), e)
    }.groupByKey()

    newEdgesWithMetaNodesAndDistUpdated.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvg(edges, false)
    }
  }

  /**
   * Create internal edges for all collapsed edges of the graph
   * @param collapsedEdges a list of edges to be collapsed
   * @param dbConnectionConfig titan configuration for writing to storage
   * @return 2 RDDs - one with internal edges and a second with non-minimal distance edges. The RDDs will be used
   *         to calculate the new active edges for the current iteration.
   */
  private def createInternalEdges(collapsedEdges: RDD[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])],
                                  dbConnectionConfig: SerializableBaseConfiguration): (RDD[HierarchicalClusteringEdge], RDD[HierarchicalClusteringEdge]) = {

    val internalEdges = collapsedEdges.mapPartitions {
      case edges: Iterator[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])] => {
        val hcStorage = HierarchicalClusteringStorage(dbConnectionConfig)

        val result = edges.map {
          case (minDistEdge, nonMinDistEdges) =>
            val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(minDistEdge, hcStorage)
            val replacedEdges = EdgeManager.createActiveEdgesForMetaNode(metanode, metanodeCount, nonMinDistEdges).map(_._2)
            (metaEdges, replacedEdges)
        }.toList

        hcStorage.commit()
        hcStorage.shutdown()

        result.toIterator
      }
    }
    internalEdges.persist(StorageLevel.MEMORY_AND_DISK)
    (internalEdges.flatMap(_._1), internalEdges.flatMap(_._2))
  }

  /**
   * Create collapsed edges for the current graph
   * @param graph the active graph at ith iteration
   * @return a list of edges to be collapsed at this iteration
   */
  private def createCollapsableEdges(graph: RDD[HierarchicalClusteringEdge]): RDD[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])] = {

    val edgesBySourceIdWithMinEdge = graph.map((e: HierarchicalClusteringEdge) => (e.src, e)).groupByKey().map {
      case (minEdge, allEdges) => EdgeDistance.min(allEdges)
    }.groupByKey().filter {
      case (minEdge,
        pairedEdgeList: Iterable[VertexOutEdges]) => EdgeManager.canEdgeCollapse(pairedEdgeList)
    }

    edgesBySourceIdWithMinEdge.map {
      case (minEdge, pairedEdgeList: Iterable[VertexOutEdges]) =>
        EdgeManager.createOutgoingEdgesForMetaNode(pairedEdgeList)
    }.filter {
      case (collapsableEdge, outgoingEdgeList) => (collapsableEdge != null)
    }
  }
}
