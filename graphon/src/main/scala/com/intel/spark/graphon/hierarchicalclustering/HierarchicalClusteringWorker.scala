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
 * @param dbConnectionConfig serializable configuration file
 */
class HierarchicalClusteringWorker(dbConnectionConfig: SerializableBaseConfiguration) extends Serializable with EventLogging {

  private val hierarchicalClusteringReport = new StringBuilder

  /**
   * Convert the storage graph into a hierarchical edge RDD
   * @param vertices the list of vertices for the initial graph
   * @param edges the list of edges for the initial graph
   */
  def execute(vertices: RDD[GBVertex], edges: RDD[GBEdge], edgeDistanceProperty: String): String = {

    val hierarchicalClusteringFactory: HierarchicalClusteringStorageFactoryInterface = HierarchicalClusteringStorageFactory(dbConnectionConfig)
    val hcRdd: RDD[HierarchicalClusteringEdge] = edges.map {
      case edge =>
        val edgeDistProperty = edge.getProperty(edgeDistanceProperty)
          .getOrElse(throw new Exception(s"Edge does not have ${edgeDistanceProperty} property"))

        HierarchicalClusteringEdge(edge.headPhysicalId.asInstanceOf[Number].longValue,
          HierarchicalClusteringConstants.DefaultNodeCount,
          edge.tailPhysicalId.asInstanceOf[Number].longValue,
          HierarchicalClusteringConstants.DefaultNodeCount,
          1 - edgeDistProperty.value.asInstanceOf[Float], false)
    }.distinct()

    configStorage(hierarchicalClusteringFactory)
    mainLoop(hcRdd, hierarchicalClusteringFactory)
    hierarchicalClusteringReport.toString()
  }

  /**
   * This is the main loop of the algorithm
   * @param graph initial in memory graph as RDD of hierarchical clustering edges
   */
  def mainLoop(graph: RDD[HierarchicalClusteringEdge],
               hcFactory: HierarchicalClusteringStorageFactoryInterface): String = {

    var currentGraph: RDD[HierarchicalClusteringEdge] = graph
    var iteration = 0

    while (currentGraph != null) {
      iteration = iteration + 1
      currentGraph = clusterNewLayer(currentGraph, iteration, hcFactory)
    }
    hierarchicalClusteringReport.toString()
  }

  /**
   * Add schema to the storage configuration
   */
  private def configStorage(hcFactory: HierarchicalClusteringStorageFactoryInterface): Unit = {

    val storage = hcFactory.newStorage()
    storage.addSchema()
    storage.shutdown()
  }

  /**
   * Creates a set of meta-node and a set of internal nodes and edges (saved to storage)
   * @param graph (n-1) in memory graph (as an RDD of hierarchical clustering edges)
   * @param iteration current iteration, testing purposes only
   * @return (n) in memory graph (as an RDD of hierarchical clustering edges)
   */
  private def clusterNewLayer(graph: RDD[HierarchicalClusteringEdge],
                              iteration: Int,
                              hcFactory: HierarchicalClusteringStorageFactoryInterface): RDD[HierarchicalClusteringEdge] = {

    // the list of edges to be collapsed and removed from the active graph
    val collapsableEdges = createCollapsableEdges(graph)
    collapsableEdges.persist(StorageLevel.MEMORY_AND_DISK)

    // the list of internal nodes connecting a newly created meta-node and the nodes of the collapsed edge
    val (internalEdges, nonSelectedEdges) = createInternalEdges(collapsableEdges, iteration, hcFactory)
    internalEdges.persist(StorageLevel.MEMORY_AND_DISK)
    nonSelectedEdges.persist(StorageLevel.MEMORY_AND_DISK)

    // the list of newly created active edges in the graph
    val activeEdges = createActiveEdges(nonSelectedEdges, internalEdges)
    activeEdges.persist(StorageLevel.MEMORY_AND_DISK)

    val iterationCountLog = HierarchicalClusteringConstants.IterationMarker + " " + iteration
    hierarchicalClusteringReport.append(iterationCountLog + "\n")
    info(iterationCountLog)

    val collapsableEdgesCount = collapsableEdges.count()
    if (collapsableEdges.count() > 0) {
      val log = "Collapsed edges " + collapsableEdgesCount
      hierarchicalClusteringReport.append(log + "\n")
      info(log)
    }
    else {
      val log = "No new collapsed edges"
      hierarchicalClusteringReport.append(log + "\n")
      info(log)
    }

    val internalEdgesCount = internalEdges.count()
    if (internalEdgesCount > 0) {
      val log = "Internal edges " + internalEdgesCount
      hierarchicalClusteringReport.append(log + "\n")
      info(log)
    }
    else {
      val log = "No new internal edges"
      hierarchicalClusteringReport.append(log + "\n")
      info(log)
    }

    val activeEdgesCount = activeEdges.count()
    if (activeEdges.count > 0) {
      internalEdges.unpersist()

      val activeEdgesLog = "Active edges " + activeEdgesCount
      hierarchicalClusteringReport.append(activeEdgesLog + "\n")
      info(activeEdgesLog)

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

      val nextItLog = "Active edges to next iteration " + distinctNewGraphWithoutInternalEdges.count()
      hierarchicalClusteringReport.append(nextItLog + "\n")
      info(nextItLog)

      distinctNewGraphWithoutInternalEdges
    }
    else {
      val log = "No new active edges - terminating..."
      hierarchicalClusteringReport.append(log + "\n")
      info(log)

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
   * @return 2 RDDs - one with internal edges and a second with non-minimal distance edges. The RDDs will be used
   *         to calculate the new active edges for the current iteration.
   */
  private def createInternalEdges(collapsedEdges: RDD[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])],
                                  iteration: Int,
                                  hcFactory: HierarchicalClusteringStorageFactoryInterface): (RDD[HierarchicalClusteringEdge], RDD[HierarchicalClusteringEdge]) = {

    val internalEdges = collapsedEdges.mapPartitions {
      case edges: Iterator[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])] => {
        val hcStorage = hcFactory.newStorage()

        val result = edges.map {
          case (minDistEdge, nonMinDistEdges) =>
            val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(minDistEdge, hcStorage, iteration)
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
