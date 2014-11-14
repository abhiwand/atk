package com.intel.spark.graphon.idassigner

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

/**
 * Renames the vertices of a graph from some arbitrary type T (that provides a ClassManifest for Spark's benefit)
 * to Long IDs.
 *
 * @tparam T type of the vertex IDs in the incoming graph
 */

class GraphIDAssigner[T: ClassTag]() extends Serializable {

  /**
   * Rename the vertices of the incoming graph from IDs of type T to Longs
   * @param inVertices vertex list of incoming graph
   * @param inEdges edges list of incoming graph
   * @return GraphIDAssignerOutput
   */
  def run(inVertices: RDD[T], inEdges: RDD[(T, T)]) = {

    val partitionVertexCounts: Array[Long] = inVertices.mapPartitions(partitionCount).collect()

    val paritionPredecessors: Array[Long] = partitionVertexCounts.scanLeft(0.toLong)(_ + _)

    val oldIdsToNew: RDD[(T, Long)] = inVertices.mapPartitionsWithIndex((i, vertices) =>
      {
        val offset: Long = paritionPredecessors.apply(i)
        vertices.zipWithIndex.map({ case (v, i) => (v, i + offset) })
      })

    oldIdsToNew.cache()

    val edgesWithSourcesRenamed : RDD[(Long, T)] =
      inEdges.join(oldIdsToNew).map({case (_, ((oldSrc, oldDst :T), newSrc)) => (newSrc, oldDst)})


    val edges : RDD[(Long,Long)] = edgesWithSourcesRenamed.map({case (x,y) => (y,x)}).join(oldIdsToNew).map(
    {case (_, ((oldDst : T, newSrc : Long), newDst: Long)) => (newSrc, newDst)})



    val newIdsToOld: RDD[(Long, T)] = oldIdsToNew.map({ case (x, y) => (y, x) })
    val newVertices = newIdsToOld.map({ case (newId, _) => newId })

    oldIdsToNew.unpersist(blocking = false)
    edgesWithSourcesRenamed.unpersist(blocking = false)
    new GraphIDAssignerOutput(newVertices, edges, newIdsToOld)
  }

  /**
   * Return value for the ID assigner.
   * @param vertices vertex list of renamed graph
   * @param edges edge list of renamed graph
   * @param newIdsToOld  pairs mapping new IDs to their corresponding vertices in the base graph
   * @tparam T Type of the vertex IDs in the input graph
   */
  case class GraphIDAssignerOutput[T: ClassTag](val vertices: RDD[Long],
                                                val edges: RDD[(Long, Long)],
                                                val newIdsToOld: RDD[(Long, T)])

  private def partitionCount(it: Iterator[T]): Iterator[Long] = Iterator(it.length)

}
