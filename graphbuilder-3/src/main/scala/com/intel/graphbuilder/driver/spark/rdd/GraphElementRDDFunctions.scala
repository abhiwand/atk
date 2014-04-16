package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements._
import org.apache.spark.rdd.RDD

/**
 * Functions applicable to RDD's of GraphElements
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 *
 * @param self input that these functions are applicable to
 */
class GraphElementRDDFunctions(self: RDD[GraphElement]) {

  /**
   * Get all of the Edges from an RDD made up of both Edges and Vertices.
   */
  def filterEdges(): RDD[Edge] = {
    self.flatMap {
      case e: Edge => Some(e)
      case _ => None
    }
  }

  /**
   * Get all of the Vertices from an RDD made up of both Edges and Vertices.
   */
  def filterVertices(): RDD[Vertex] = {
    self.flatMap {
      case v: Vertex => Some(v)
      case _ => None
    }
  }
}
