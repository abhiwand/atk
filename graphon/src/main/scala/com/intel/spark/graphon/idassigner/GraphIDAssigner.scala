package com.intel.spark.graphon.idassigner

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


case class GraphIDAssignerOutput[T : ClassManifest](val vertices: RDD[Long],
                                                    val edges: RDD[(Long, Long)],
                                                    val vertexDecoder: RDD[(Long, T)])

class GraphIDAssigner[T : ClassManifest](sc: SparkContext) extends Serializable {

  def run(inVertices: RDD[T], inEdges: RDD[(T, T)]) = {


    // place the vertices in a bucket

    val verticesGroupedByHashCodes = inVertices.map(v => (v.hashCode(), v)).groupBy(_._1).map(p => p._2)
    val hashGroupsWithPositions = verticesGroupedByHashCodes.map(seq => seq.zip(1 to seq.size)).flatMap(identity)

    val newIdToOld = hashGroupsWithPositions.map(
    { case ((hashCode, vertex), bucketPosition) => ((hashCode.toLong << 32) + bucketPosition.toLong, vertex)})

    val oldIdToNew = newIdToOld.map({case (newId,baseVertex) => (baseVertex,newId)})

    val newVertices = newIdToOld.map({ case (newId, vertex) => newId})





    val edgesWithSourcesRenamed  = inEdges.cogroup(newIdToOld
      .map({case (newId,baseVertex ) => (baseVertex,newId)}))
    .map(_._2)
    .flatMap( { case (dstList, idList) => dstList.flatMap( dst => idList.map(srcId => (srcId,dst)))})

    val edges  = edgesWithSourcesRenamed
      .map ( {case (srcWithNewName, dstWithOldName) => (dstWithOldName, srcWithNewName)})
      .cogroup(newIdToOld
      .map({case (newId,baseVertex) => (baseVertex,newId)}))
      .map(_._2)
      .flatMap( { case (srcList, idList) => srcList.flatMap( src => idList.map(dstId => (src, dstId)))})



    new GraphIDAssignerOutput(newVertices, edges, newIdToOld)
  }
}
