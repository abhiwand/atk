package com.intel.intelanalytics.engine.spark.frame.plugins.hierarchicalclustering

import java.lang

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.schema.{ PropertyType, PropertyDef, EdgeLabelDef, GraphSchema }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.write.titan.TitanSchemaWriter
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Edge, Vertex }
import org.apache.spark.rdd.RDD
import java.io.{ Serializable, FileWriter }
import org.apache.spark.SparkContext._

import scala.io.Source
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
object HierarchicalClusteringImpl extends Serializable {

  def execute(vertices: RDD[GBVertex], edges: RDD[GBEdge], titanConfig: SerializableBaseConfiguration): Unit = {

    val graphAdRdd: RDD[HierarchicalClusteringEdge] = edges.map {
      case e => HierarchicalClusteringEdge(e.headPhysicalId.asInstanceOf[Number].longValue,
        1,
        e.tailPhysicalId.asInstanceOf[Number].longValue,
        1,
        e.getProperty("dist").get.value.asInstanceOf[Float], false)
    }.distinct()

    HierarchicalGraphClustering.execute(graphAdRdd, titanConfig)
  }
}

object TitanStorage extends Serializable {

  def connectToTitan(titanConfig: SerializableBaseConfiguration): TitanGraph = {
    val titanConnector = new TitanGraphConnector(titanConfig)
    titanConnector.connect()
  }

  def addSchemaToTitan(titanGraph: TitanGraph): Unit = {

    val schema = new GraphSchema(List(EdgeLabelDef("cluster")),
      List(PropertyDef(PropertyType.Vertex, "_label", classOf[String]),
        PropertyDef(PropertyType.Vertex, "count", classOf[Int]),
        PropertyDef(PropertyType.Vertex, "name", classOf[String])))
    val schemaWriter = new TitanSchemaWriter(titanGraph)

    schemaWriter.write(schema)
  }

  def addVertexToTitan(nodeCount: Int,
                       name: String,
                       titanGraph: TitanGraph): Vertex = {

    val vertex = titanGraph.addVertex(null)
    vertex.setProperty("_label", "cluster")
    vertex.setProperty("count", nodeCount)

    //
    // TODO: this is testing only, remove later.
    //
    vertex.setProperty("name", name)

    vertex
  }

  def addEdgeToTitan(src: Vertex, dest: Vertex,
                     titanGraph: TitanGraph): Edge = {
    titanGraph.addEdge(null, src, dest, "cluster")

  }

  def commit(titanGraph: TitanGraph): Unit = {

    titanGraph.commit()
  }

  def shutDown(titanGraph: TitanGraph): Unit = {

    titanGraph.shutdown()
  }

  /**
   * TODO: this is for testing purposes only. Remove later
   * @param edge
   * @return
   */
  def getInMemoryVertextName(edge: HierarchicalClusteringEdge): String = {

    edge.src.toString + "_" + edge.dest.toString
  }
}

/**
 * This is the edge distance class.
 */
object EdgeDistance extends Serializable {
  def min(edgeList: Iterable[HierarchicalClusteringEdge]): (Object, HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge]) = {

    var dist: Float = Int.MaxValue
    var edgeWithMinDist: HierarchicalClusteringEdge = null
    var nonMinDistEdges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()

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
              if (edge.src.toString < edgeWithMinDist.src.toString) {
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
        if (edgeWithMinDist.dest.toString < edgeWithMinDist.src.toString) {
          //
          // swap the node ids so the smaller node is always source
          //
          val temp = edgeWithMinDist.src
          edgeWithMinDist.src = edgeWithMinDist.dest
          edgeWithMinDist.dest = temp
        }

        (edgeWithMinDist.src.toString + edgeWithMinDist.dest.toString, edgeWithMinDist, nonMinDistEdges)
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

  //
  // Sum (edgeDistance)
  // -------------------
  // Total edges in the Iterable
  //
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

  //
  // Same as simpleAvg + node swaps on the "head edge"
  //
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

/**
 * This is the edge manager class.
 */
object EdgeManager extends Serializable {
  /**
   *
   * @param list a  list of iterable edges. If the list has 2 elements, the head element (an edge) of any of the lists can collapse
   * @return true if the edge can collapse; false otherwise
   */
  def canEdgeCollapse(list: Iterable[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])]): Boolean = {
    (null != list) && (list.toArray.length > 1)
  }

  /**
   * Replace a node with associated meta-node in edges.
   * @param edgeList - a list with the following property:
   *                 head - an edge with the meta node as source node
   *                 tail - a list of edges whose destination nodes will need to be replaced
   * @return the edge list (less the head element) with the destination node replaced by head.source
   */
  def replaceWithMetaNode(edgeList: Iterable[HierarchicalClusteringEdge]): Iterable[HierarchicalClusteringEdge] = {

    if (edgeList.toArray.length > 1) {
      var internalEdge: HierarchicalClusteringEdge = null
      for (edge <- edgeList) {
        if (edge.isInternal) {
          internalEdge = edge
        }
      }
      if (null != internalEdge) {
        for (edge <- edgeList) {
          if (edge != internalEdge) {
            edge.distance = edge.distance * edge.destNodeCount
            edge.dest = internalEdge.src
            edge.destNodeCount = internalEdge.srcNodeCount
          }
        }
      }
    }

    edgeList.filter(e => e.isInternal == false)
  }

  /**
   * Creates a flat list of edges (to be interpreted as outgoing edges) for a meta-node
   * @param list a list of (lists of) edges. The source node of the head element of each list is the metanode
   * @return a flat list of outgoing edges for metanode
   */
  def createOutgoingEdgesForMetaNode(list: Iterable[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])]): (HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge]) = {

    var outgoingEdges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()
    var edge: HierarchicalClusteringEdge = null

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
  def createInternalEdgesForMetaNode(edge: HierarchicalClusteringEdge,
                                     graph: TitanGraph): (Long, Int, List[HierarchicalClusteringEdge]) = {
    var edges: List[HierarchicalClusteringEdge] = List[HierarchicalClusteringEdge]()
    var metaNodeVertexId: Long = 0

    if (null != edge) {

      val metaNodeVertex = TitanStorage.addVertexToTitan(edge.getTotalNodeCount,
        TitanStorage.getInMemoryVertextName(edge),
        graph)
      metaNodeVertexId = metaNodeVertex.getId.asInstanceOf[Long]

      TitanStorage.addEdgeToTitan(
        metaNodeVertex,
        graph.getVertex(edge.src),
        graph)

      TitanStorage.addEdgeToTitan(
        metaNodeVertex,
        graph.getVertex(edge.dest),
        graph)

      edges = edges :+ HierarchicalClusteringEdge(metaNodeVertexId,
        edge.getTotalNodeCount,
        edge.src,
        edge.srcNodeCount,
        1, true)
      edges = edges :+ HierarchicalClusteringEdge(metaNodeVertexId,
        edge.getTotalNodeCount,
        edge.dest,
        edge.destNodeCount,
        1, true)
    }

    (metaNodeVertexId, edge.getTotalNodeCount, edges)
  }

  /**
   * Creates a list of active edges for meta-node
   * @param metaNode
   * @param count
   * @param nonSelectedEdges
   * @return
   */
  def createActiveEdgesForMetaNode(metaNode: Long, count: Int,
                                   nonSelectedEdges: Iterable[HierarchicalClusteringEdge]): List[((Long, Int), HierarchicalClusteringEdge)] = {

    nonSelectedEdges.map(e => ((e.dest, e.destNodeCount),
      HierarchicalClusteringEdge(
        metaNode,
        count,
        e.dest,
        e.destNodeCount,
        e.distance, false))).toList
  }
}

/**
 * This is the main clustering class.
 */
object HierarchicalGraphClustering extends Serializable {

  def execute(graph: RDD[HierarchicalClusteringEdge], titanConfig: SerializableBaseConfiguration): Unit = {

    var currentGraph: RDD[HierarchicalClusteringEdge] = graph
    val fileWriter = new FileWriter("test.txt", true)

    fileWriter.write("Initial graph\n")
    currentGraph.collect().foreach((e: HierarchicalClusteringEdge) => fileWriter.write(e.toString() + "\n"))
    fileWriter.write("\n\n")
    fileWriter.flush()

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
      internalEdges.collect().foreach((e: HierarchicalClusteringEdge) => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("Internal edges - done\n\n")
    }
    else {
      fileWriter.write("No new internal edges\n")
    }
    fileWriter.flush()

    if (nonSelectedEdges.count() > 0) {
      fileWriter.write("Non-selected edges - start\n")
      nonSelectedEdges.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("Non-selected edges - done\n\n")
    }
    else {
      fileWriter.write("No new non-selected edges\n")
    }
    fileWriter.flush()

    if (activeEdges.count > 0) {
      internalEdges.unpersist()

      //double the edges for edge selection algorithm
      val activeEdgesBothDirections = activeEdges.flatMap((e: HierarchicalClusteringEdge) => Seq(e, HierarchicalClusteringEdge(e.dest,
        e.destNodeCount,
        e.src,
        e.srcNodeCount,
        e.distance, e.isInternal)))

      // create a key-value pair list of edges from the current graph (for subtractByKey)
      val currentGraphAsKVPair = graph.map((e: HierarchicalClusteringEdge) => (e.src, e))

      // create a key-value pair list of edges from the list of edges to be collapsed for subtractByKey)
      val collapsedEdgesAsKVPair = collapsableEdges.flatMap {
        case (collapsedEdge, nonSelectedEdges) => Seq((collapsedEdge.src, null),
          (collapsedEdge.dest, null))
      }

      //remove collapsed edges from the active graph - by src node
      val newGraphReducedBySrc = currentGraphAsKVPair.subtractByKey(collapsedEdgesAsKVPair).values

      //remove collapsed edges from the active graph - by dest node
      val newGraphReducedBySrcAndDest = newGraphReducedBySrc.map((e: HierarchicalClusteringEdge) => (e.dest, e)).subtractByKey(collapsedEdgesAsKVPair).values
      val newGraphWithoutInternalEdges = activeEdgesBothDirections.union(newGraphReducedBySrcAndDest).coalesce(activeEdgesBothDirections.partitions.length, true)

      newGraphWithoutInternalEdges.cache()
      collapsableEdges.unpersist()
      activeEdges.unpersist()

      fileWriter.write("Active new edges - start\n")
      newGraphWithoutInternalEdges.collect().foreach((e: HierarchicalClusteringEdge) => fileWriter.write("\t" + e.toString() + "\n"))
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
   * @param nonSelectedEdges - the set of collapsed edges
   * @param internalEdges - the set of internal edges (previously calculated from collapsed ones)
   * @return a list of new edges (containing meta-nodes) to be added to the active graph. The edge distance is updated/calculated for the new edges
   */
  private def createActiveEdges(nonSelectedEdges: RDD[HierarchicalClusteringEdge],
                                internalEdges: RDD[HierarchicalClusteringEdge], fileWriter: FileWriter): RDD[HierarchicalClusteringEdge] = {
    val edgeManager = EdgeManager

    val activeEdges = nonSelectedEdges.map {
      case (e) => ((e.src, e.dest, e.destNodeCount), e)
    }.groupByKey()

    if (activeEdges.count() > 0) {
      fileWriter.write("activeEdges- start\n")
      activeEdges.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("activeEdges - done\n\n")
    }
    else {
      fileWriter.write("No activeEdges\n")
    }
    fileWriter.flush()

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

    if (activeEdgesWithWeightedAvgDistance.count() > 0) {
      fileWriter.write("activeEdgesWithWeightedAvgDistance- start\n")
      activeEdgesWithWeightedAvgDistance.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("activeEdgesWithWeightedAvgDistance - done\n\n")
    }
    else {
      fileWriter.write("No activeEdgesWithWeightedAvgDistance\n")
    }
    fileWriter.flush()

    val newEdges = (internalEdges union activeEdgesWithWeightedAvgDistance).coalesce(internalEdges.partitions.length, true).map(
      (e: HierarchicalClusteringEdge) => (e.dest, e)
    ).groupByKey()

    if (newEdges.count() > 0) {
      fileWriter.write("newEdges- start\n")
      newEdges.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("newEdges - done\n\n")
    }
    else {
      fileWriter.write("No newEdges\n")
    }
    fileWriter.flush()

    // update the dest node with meta-node in the list
    val newEdgesWithMetaNodeForDest = newEdges.map {
      case (dest, newEdges) => edgeManager.replaceWithMetaNode(newEdges)
    }.flatMap(identity)

    if (newEdgesWithMetaNodeForDest.count() > 0) {
      fileWriter.write("newEdgesWithMetaNodeForDest- start\n")
      newEdgesWithMetaNodeForDest.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("newEdgesWithMetaNodeForDest - done\n\n")
    }
    else {
      fileWriter.write("No newEdgesWithMetaNodeForDest\n")
    }
    fileWriter.flush()

    val newEdgesWithMetaNodeGrouped = newEdgesWithMetaNodeForDest.map(
      (e: HierarchicalClusteringEdge) => ((e.src, e.dest), e)
    ).groupByKey()

    if (newEdgesWithMetaNodeGrouped.count() > 0) {
      fileWriter.write("newEdgesWithMetaNodeGrouped- start\n")
      newEdgesWithMetaNodeGrouped.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("newEdgesWithMetaNodeGrouped - done\n\n")
    }
    else {
      fileWriter.write("No newEdgesWithMetaNodeGrouped\n")
    }
    fileWriter.flush()

    // recalculate the edge distance if several outgoing edges go into the same meta-node
    val newEdgesWithMetaNodesAndDistUpdated = newEdgesWithMetaNodeGrouped.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvgWithNodeSWap(edges)
    }.map {
      (e: HierarchicalClusteringEdge) => ((e.src, e.dest), e)
    }.groupByKey()

    if (newEdgesWithMetaNodesAndDistUpdated.count() > 0) {
      fileWriter.write("newEdgesWithMetaNodesAndDistUpdated- start\n")
      newEdgesWithMetaNodesAndDistUpdated.collect().foreach(e => fileWriter.write("\t" + e.toString() + "\n"))
      fileWriter.write("newEdgesWithMetaNodesAndDistUpdated - done\n\n")
    }
    else {
      fileWriter.write("No newEdgesWithMetaNodesAndDistUpdated\n")
    }
    fileWriter.flush()

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

    println("Collapsable before\n")
    collapsedEdges.collect().foreach(e => println(e._1.toString() + "\n"))
    println("Collapsable end\n")

    val result = collapsedEdges.mapPartitions {
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
    result.cache()
    (result.flatMap(_._1), result.flatMap(_._2))
  }

  /**
   * Create collapsed edges for the current graph
   * @param graph the active graph at ith iteration
   * @return a list of edges to be collapsed at this iteration
   */
  private def createCollapsableEdges(graph: RDD[HierarchicalClusteringEdge]): RDD[(HierarchicalClusteringEdge, Iterable[HierarchicalClusteringEdge])] = {

    val edgeManager = EdgeManager

    val collapsableEdges = graph.map((e: HierarchicalClusteringEdge) => (e.src, e)).groupByKey().map {
      case (sourceNode, allEdges) =>
        val min = EdgeDistance.min(allEdges)
        min match {
          case (vertexId,
            minEdge,
            nonSelectedEdges) => (vertexId, (minEdge, nonSelectedEdges))
        }
    }.groupByKey().filter {
      case (vertexId, pairedEdgeList) => edgeManager.canEdgeCollapse(pairedEdgeList)
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
