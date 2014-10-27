package org.apache.spark.ia.graph

import com.intel.intelanalytics.domain.schema.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Additional functions for RDD's of type Vertex
 * @param parent the RDD to wrap
 */
class VertexRDDFunctions(parent: RDD[Vertex]) {

  /**
   * Vertex frames from parent based on the labels provided
   * @param schemas by providing the list of schemas you can prevent an extra map/reduce to collect them
   * @return the VertexFrameRDD - one vertex type per RDD
   */
  def splitByLabel(schemas: List[Schema]): List[VertexFrameRDD] = {
    parent.cache()

    val split = schemas.map(_.label.get).map(label => parent.filter(vertex => vertex.label() == label))
    split.foreach(_.cache())

    val rowRdds = split.map(rdd => rdd.map(vertex => vertex.row))

    val results = schemas.zip(rowRdds).map { case (schema: Schema, rows: RDD[Row]) => new VertexFrameRDD(schema, rows) }

    parent.unpersist(blocking = false)
    split.foreach(_.unpersist(blocking = false))
    results
  }

  /**
   * Split parent (mixed vertex types) into a list of vertex frames (one vertex type per frame)
   *
   * IMPORTANT! does not perform as well as splitByLabel(schemas) - extra map() and distinct()
   */
  def splitByLabel(): List[VertexFrameRDD] = {
    parent.cache()
    val schemas = parent.map(vertex => vertex.schema).distinct().collect().toList
    splitByLabel(schemas)
  }

}
