package com.intel.spark.graphon.iatpregel

import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object IATPregel {

  /**
   * Implements Pregel-like BSP message passing. It is the GraphX implementation of Pregel extended with richer logging.
   *
   * @param graph The graph on which to run the Pregel program. A GraphX graph.
   * @param initialMsg The initial message to be sent to every vertex at the start of the computation.
   * @param initialReportGenerator Function that creates the initial summary of vertex and edge data for the log.
   * @param superStepStatusGenerator Function that creates the per-superstep report for the log. It consumes only vertex
   *                                 data because Pregel programs do not modify edge data.
   * @param maxIterations The maximum number of supersteps that can be executed in this run.
   * @param activeDirection The direction of edges incident to a vertex that received a message in
   * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
   * out-edges of vertices that received a message in the previous round will run. The default is
   * `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
   * in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
   * *both* vertices received a message.
   * @param vprog The user-defined vertex program which runs on each
   * vertex and receives the inbound message and computes a new vertex
   * value. On the first iteration the vertex program is invoked on
   * all vertices and is passed the default message. On subsequent
   * iterations the vertex program is only invoked on those vertices
   * that receive messages.
   * @param sendMsg A user supplied function that is applied to out
   * edges of vertices that received messages in the current
   * iteration.
   * @param mergeMsg A user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type
   * A. ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   * @tparam VertexData Class of the per-vertex data in the computation.
   * @tparam EdgeData Class of the per-edge data in the computation
   * @tparam Message Message type passed during the progress of the
   * @return Pair of GraphX graph (with updated values) and log string.
   */
  def apply[VertexData: ClassTag, EdgeData: ClassTag, Message: ClassTag](graph: Graph[VertexData, EdgeData],
                                                                         initialMsg: Message,
                                                                         initialReportGenerator: InitialReport[VertexData, EdgeData],
                                                                         superStepStatusGenerator: SuperStepStatusGenerator[VertexData],
                                                                         maxIterations: Int = Int.MaxValue,
                                                                         activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VertexData, Message) => VertexData,
                                                                                                                                sendMsg: EdgeTriplet[VertexData, EdgeData] => Iterator[(VertexId, Message)],
                                                                                                                                mergeMsg: (Message, Message) => Message): (Graph[VertexData, EdgeData], String) = {
    val vdataRDD: RDD[VertexData] = graph.vertices.map({ case (vid, vdata) => vdata })
    val edataRDD: RDD[EdgeData] = graph.edges.map({ case e: Edge[EdgeData] => e.attr })

    val initialReport = initialReportGenerator.generateInitialReport(vdataRDD, edataRDD)

    var log = new StringBuilder(initialReport)

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()

    // Loop
    var prevG: Graph[VertexData, EdgeData] = null
    var i = 1

    val status = superStepStatusGenerator.generateSuperStepStatus(i, g.vertices.map({ case (vid, vdata) => vdata }))
    var earlyTermination = status.earlyTermination

    while (activeMessages > 0 && i < maxIterations && !earlyTermination) {
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

      val status = superStepStatusGenerator.generateSuperStepStatus(i, g.vertices.map({ case (vid, vdata) => vdata }))
      log.++=(status.log)
      earlyTermination = status.earlyTermination

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

// end of class IATPregel
