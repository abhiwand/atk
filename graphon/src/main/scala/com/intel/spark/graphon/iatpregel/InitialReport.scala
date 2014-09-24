package com.intel.spark.graphon.iatpregel

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * Implementations of this trait provide a method for creating an initial status report for a Pregel-run using the
 * incoming edge and vertex RDDs.
 * @tparam V Class of the vertex data in the graph.
 * @tparam E Class of the edge data in the graph.
 */
trait InitialReport[V, E] {
  def generateInitialReport(vertices: RDD[V], edges: RDD[E]): String
}
