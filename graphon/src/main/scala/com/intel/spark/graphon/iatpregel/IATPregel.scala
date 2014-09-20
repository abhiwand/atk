package com.intel.spark.graphon.iatpregel

import scala.reflect.ClassTag

import org.apache.spark.graphx._

object IATPregel {

  /**
   * Implements Pregel-like BSP message passing.
   * Based on the GraphX implementation of Pregel but with richer logging.
   *
   * @param graph
   * @param initialMsg
   * @param pregelLogger
   * @param maxIterations
   * @param activeDirection
   * @param vprog
   * @param sendMsg
   * @param mergeMsg
   * @tparam VertexData
   * @tparam EdgeData
   * @tparam Message
   * @tparam VertexInitialState
   * @tparam EdgeInitialState
   * @tparam SuperStepState
   * @return
   */
  def apply[VertexData: ClassTag, EdgeData: ClassTag, Message: ClassTag, VertexInitialState: ClassTag, EdgeInitialState: ClassTag, SuperStepState: ClassTag](graph: Graph[VertexData, EdgeData],
                                                                                                                                                             initialMsg: Message,
                                                                                                                                                             pregelLogger: IATPregelLogger[VertexData, VertexInitialState, EdgeData, EdgeInitialState, SuperStepState],
                                                                                                                                                             maxIterations: Int = Int.MaxValue,
                                                                                                                                                             activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VertexData, Message) => VertexData,
                                                                                                                                                                                                                    sendMsg: EdgeTriplet[VertexData, EdgeData] => Iterator[(VertexId, Message)],
                                                                                                                                                                                                                    mergeMsg: (Message, Message) => Message): (Graph[VertexData, EdgeData], String) = {
    val sparkContext = graph.vertices.sparkContext // GraphX should put spark context at the graph level.

    val emptyVertexInitialStatus = pregelLogger.emptyVertexInitialStatus
    val vertexDataToInitialStatus = pregelLogger.vertexDataToInitialStatus
    val vertexInitialStatusCombiner = pregelLogger.vertexInitialStatusCombiner

    val emptyEdgeInitialStatus = pregelLogger.emptyEdgeInitialStatus
    val edgeDataToInitialStatus = pregelLogger.edgeDataToInitialStatus
    val edgeInitialStatusCombiner = pregelLogger.edgeInitialStatusCombiner
    val generateInitialReport = pregelLogger.generateInitialReport
    val accumulateStepStatus = pregelLogger.accumulateStepStatus
    val convertStateToStatus = pregelLogger.convertStateToStatus
    val generatePerStepReport = pregelLogger.generatePerStepReport

    val vInitial = (sparkContext.parallelize(List(emptyVertexInitialStatus)).union(graph.vertices.map({ case (vid, vdata) => vertexDataToInitialStatus(vdata) }))).reduce(vertexInitialStatusCombiner)

    val eInitial = (sparkContext.parallelize((List(emptyEdgeInitialStatus))).union(graph.edges.map({ case e: Edge[EdgeData] => edgeDataToInitialStatus(e.attr) }))).reduce(edgeInitialStatusCombiner)

    var log = new StringBuilder(generateInitialReport(vInitial, eInitial))

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()

    // Loop
    var prevG: Graph[VertexData, EdgeData] = null
    var i = 1

    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.cache()

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()

      log.++=(generatePerStepReport(g.vertices.map({ case (id, data) => convertStateToStatus(data) }).reduce(accumulateStepStatus), i))

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      newVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }

    (g, log.toString())
  } // end of apply

}

// end of class Pregel
