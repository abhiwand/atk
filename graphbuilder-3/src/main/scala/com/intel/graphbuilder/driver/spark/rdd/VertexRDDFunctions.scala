package com.intel.graphbuilder.driver.spark.rdd

import com.intel.graphbuilder.elements.{GbIdToPhysicalId, Vertex}
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Functions that are applicable to Vertex RDD's.
 * <p>
 * This is best used by importing GraphBuilderRDDImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */
class VertexRDDFunctions(self: RDD[Vertex]) {

  /**
   * Merge duplicate Vertices, creating a new Vertex that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[Vertex] = {
    self.groupBy(m => m.id).mapValues(dups => dups.reduce((m1, m2) => m1.merge(m2))).values
  }

  /**
   * Write to Titan and produce a mapping of GbId's to Physical Id's
   * <p>
   * This is an unusual transformation because it has the side effect of writing to Titan.
   * This means extra care is needed to prevent it from being recomputed.
   * </p>
   */
  def write(titanConnector: TitanGraphConnector, append: Boolean = false): RDD[GbIdToPhysicalId] =
    new TitanVertexWriterRDD(self, titanConnector, append)

}
