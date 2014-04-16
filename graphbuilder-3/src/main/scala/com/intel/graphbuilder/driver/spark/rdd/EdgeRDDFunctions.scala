package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.write.EdgeWriter
import com.intel.graphbuilder.write.dao.{VertexDAO, EdgeDAO}
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

/**
 * Functions that are applicable to Edge RDD's
 *
 * This is best used by importing GraphBuilderRDDImplicits._
 *
 * @param self input that these functions are applicable to
 */
class EdgeRDDFunctions(self: RDD[Edge]) extends Serializable {

  /**
   * Merge duplicate Edges, creating a new Edge that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[Edge] = {
    self.groupBy(m => m.id).mapValues(dups => dups.reduce((m1, m2) => m1.merge(m2))).values
  }

  /**
   * For every Edge, create an additional Edge going in the opposite direction. This
   * method will double the size of the RDD.
   * <p>
   * Depending on the source data, duplicates may be created in this process. So you may
   * need to merge duplicates after.
   * </p>
   */
  def biDirectional(): RDD[Edge] = {
    self.flatMap(edge => List(edge, edge.reverse()))
  }

  /**
   * For every Edge, create two vertices, one from the tail Vertex GbId, and one from the
   * head Vertex GbId.
   * <p>
   * This functionality was called "retain dangling edges" in GB version 2.
   * </p>
   */
  def verticesFromEdges(): RDD[Vertex] = {
    self.flatMap(edge => List(new Vertex(edge.tailVertexGbId, Nil), new Vertex(edge.headVertexGbId, Nil)))
  }

  /**
   * Join Edges with the Physical Id's so they won't need to be looked up when writing Edges.
   *
   * This is an inner join.  Edges that can't be joined are dropped.
   */
  def joinWithPhysicalIds(ids: RDD[GbIdToPhysicalId]): RDD[Edge] = {
    //TODO: can this be simplified?

    val idsByGbId = ids.groupBy(idMap => idMap.gbId)

    // set physical ids for tail vertices
    val edgesByTail = self.groupBy(edge => edge.tailVertexGbId)
    val edgesWithTail = idsByGbId.join(edgesByTail).flatMapValues(value => {
      val gbIdToPhysicalIds = value._1
      val physicalId = gbIdToPhysicalIds(0).physicalId
      val edges = value._2
      edges.map(e => e.copy(tailPhysicalId = physicalId))
    }).values

    // set physical ids for head vertices
    val edgesByHead = edgesWithTail.groupBy(e => e.headVertexGbId)
    val edgesWithPhysicalIds = idsByGbId.join(edgesByHead).flatMapValues(value => {
      val gbIdToPhysicalIds = value._1
      val physicalId = gbIdToPhysicalIds(0).physicalId
      val edges = value._2
      edges.map(e => e.copy(headPhysicalId = physicalId))
    }).values

    edgesWithPhysicalIds
  }

  /**
   * Filter Edges that do NOT have physical ids
   */
  def filterEdgesWithoutPhysicalIds(): RDD[Edge] = {
    self.filter(edge => {
      if (edge.tailPhysicalId == null || edge.headPhysicalId == null) false
      else true
    })
  }

  /**
   * Write the Edges to Titan using the supplied connector
   * @param append true to append to an existing graph
   */
  def write(titanConnector: TitanGraphConnector, append: Boolean): Unit = {

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[Edge]) => {
      val graph = titanConnector.connect()
      val edgeDAO = new EdgeDAO(graph, new VertexDAO(graph))
      val writer = new EdgeWriter(edgeDAO, append)

      try {
        var count = 0L
        while (iterator.hasNext) {
          writer.write(iterator.next())
          count += 1
        }

        println("wrote edges: " + count + " for split: " + context.partitionId)

        graph.commit()
      }
      finally {
        graph.shutdown()
      }
    })
  }
}
