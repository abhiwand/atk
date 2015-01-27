package com.intel.spark.graphon.clusteringcoefficient

import com.intel.spark.graphon.graphxutilities.OpenHashSet
import org.apache.spark.graphx._

import scala.collection
import scala.collection.parallel.mutable
import scala.reflect.ClassTag

/**
 * Compute the local clustering coefficient of each vertex in a graph.
 *
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
 * using [[org.apache.spark.graphx.Graph#partitionBy]].
 */
object GraphXClusteringCoefficient {

  type VertexSet = OpenHashSet[Long]

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): (Graph[Double, ED], Double) = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[VertexSet, ED]): Iterator[(VertexId, Int)] = {
      assert(et.srcAttr != null)
      assert(et.dstAttr != null)
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      }
      else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      Iterator((et.srcId, counter), (et.dstId, counter))
    }
    // compute the intersection along edges

    val triangleDoubleCounts: VertexRDD[Int] = setGraph.mapReduceTriplets(edgeFunc, _ + _)

    val degreesChooseTwo: Graph[Long, ED] = setGraph.mapVertices({ case (vid, vertexSet) => (chooseTwo(vertexSet.size)) })

    val doubleCountOfTriangles: Long =
      triangleDoubleCounts.aggregate[Long](0L)({ case (x: Long, (vid: VertexId, td: Int)) => x + td.toLong }, _ + _)

    val totalDegreesChooseTwo: Long =
      degreesChooseTwo.vertices.aggregate[Long](0L)({ case (x: Long, (vid: VertexId, dc2: Long)) => x + dc2 }, _ + _)

    val globalClusteringCoefficient = if (totalDegreesChooseTwo > 0) {
      (doubleCountOfTriangles / 2.0d) / totalDegreesChooseTwo.toDouble
    }
    else {
      1.0d
    }

    val localClusteringCoefficientGraph = degreesChooseTwo.outerJoinVertices(triangleDoubleCounts) {
      (vid, dC2, optCounter: Option[Int]) =>
        if (dC2 == 0L) {
          1.0d
        }
        else {
          val triangleDoubleCount = optCounter.getOrElse(0)
          (triangleDoubleCount.toDouble / 2.0d) / dC2.toDouble
        }
    }

    (localClusteringCoefficientGraph, globalClusteringCoefficient)

  }

  private def chooseTwo(n: Long): Long = (n * (n - 1)) / 2

}
