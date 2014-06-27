package com.intel.spark.graphon.idassigner

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Renames the vertices of a graph from some arbitrary type T (that provides a ClassManifest for Spark's benefit)
 * to Long IDs.
 *
 * @param sc spark context
 * @tparam T type of the vertex IDs in the incoming graph
 */

class GraphIDAssigner[T: ClassManifest](sc: SparkContext) extends Serializable {

  /**
   * Rename the vertices of the incoming graph from IDs of type T to Longs
   * @param inVertices vertex list of incoming graph
   * @param inEdges edges list of incoming graph
   * @return GraphIDAssignerOutput
   */
  def run(inVertices: RDD[T], inEdges: RDD[(T, T)]) = {

    val verticesGroupedByHashCodes = inVertices.map(v => (v.hashCode(), v)).groupBy(_._1).map(p => p._2)

    val hashGroupsWithPositions = verticesGroupedByHashCodes.flatMap(seq => seq.zip(1 to seq.size))

    val newIdsToOld = hashGroupsWithPositions.map(
      { case ((hashCode, vertex), bucketPosition) => ((hashCode.toLong << 32) + bucketPosition.toLong, vertex) })

    val oldIdsToNew = newIdsToOld.map({ case (newId, oldId) => (oldId, newId) })

    val newVertices = newIdsToOld.map({ case (newId, _) => newId })

    val edgesGroupedWithNewIdsOfSources = inEdges.cogroup(oldIdsToNew).map(_._2)

    // the id list is always a singleton list because there is one new ID for each incoming vertex
    // this keeps the serialization of the closure relatively small

    val edgesWithSourcesRenamed = edgesGroupedWithNewIdsOfSources.
      flatMap({ case (dstList, srcIdList) => dstList.flatMap(dst => srcIdList.map(srcId => (srcId, dst))) })

    val partlyRenamedEdgesGroupedWithNewIdsOfDestinations = edgesWithSourcesRenamed
      .map({ case (srcWithNewId, dstWithOldId) => (dstWithOldId, srcWithNewId) })
      .cogroup(oldIdsToNew).map(_._2)

    // the id list is always a singleton list because there is one new ID for each incoming vertex
    // this keeps the serialization of the closure relatively small

    val edges = partlyRenamedEdgesGroupedWithNewIdsOfDestinations
      .flatMap({ case (srcList, idList) => srcList.flatMap(src => idList.map(dstId => (src, dstId))) })

    new GraphIDAssignerOutput(newVertices, edges, newIdsToOld)
  }

  /**
   * Return value for the ID assigner.
   * @param vertices vertex list of renamed graph
   * @param edges edge list of renamed graph
   * @param newIdsToOld  pairs mapping new IDs to their corresponding vertices in the base graph
   * @tparam T Type of the vertex IDs in the input graph
   */
  case class GraphIDAssignerOutput[T: ClassManifest](val vertices: RDD[Long],
                                                     val edges: RDD[(Long, Long)],
                                                     val newIdsToOld: RDD[(Long, T)])

}
