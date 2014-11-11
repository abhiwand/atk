package org.apache.spark.ia.graph

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, Schema }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.reflect.ClassTag

/**
 * Edge list for a "Seamless" Graph
 *
 * @param schema  the schema describing the columns of this frame
 * @param sqlContext a spark SQLContext
 * @param logicalPlan a logical plan describing the SchemaRDD
 */
class EdgeFrameRDD(schema: Schema,
                   sqlContext: SQLContext,
                   logicalPlan: LogicalPlan) extends FrameRDD(schema, sqlContext, logicalPlan) {

  def this(frameRDD: FrameRDD) = this(frameRDD.frameSchema, frameRDD.sqlContext, frameRDD.logicalPlan)

  def this(schema: Schema, rowRDD: RDD[sql.Row]) = this(schema, new SQLContext(rowRDD.context), FrameRDD.createLogicalPlanFromSql(schema, rowRDD))

  /** Edge wrapper provides richer API for working with Vertices */
  val edge = new EdgeWrapper(schema)

  /**
   * Map over edges
   * @param mapFunction map function that operates on a EdgeWrapper
   * @tparam U return type that will be the in resulting RDD
   */
  def mapEdges[U: ClassTag](mapFunction: (EdgeWrapper) => U): RDD[U] = {
    this.map(data => {
      mapFunction(edge(data))
    })
  }

  /**
   * Convert this RDD in match the schema provided
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  override def convertToNewSchema(updatedSchema: Schema): EdgeFrameRDD = {
    if (schema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new EdgeFrameRDD(super.convertToNewSchema(updatedSchema))
    }
  }

  /**
   * Map over all edges and assign the label from the schema
   */
  def assignLabelToRows(): EdgeFrameRDD = {
    new EdgeFrameRDD(schema, mapEdges(edge => edge.setLabel(schema.label.get)))
  }

  /**
   * Append edges to the current frame:
   * - union the schemas to match, if needed
   * - no overwrite
   */
  def append(other: FrameRDD): EdgeFrameRDD = {
    val unionedSchema = schema.union(other.frameSchema).reorderColumns(GraphSchema.edgeSystemColumnNames)

    // TODO: better way to check for empty?
    if (take(1).length > 0) {
      val part1 = convertToNewSchema(unionedSchema)
      val part2 = new EdgeFrameRDD(other.convertToNewSchema(unionedSchema))
      new EdgeFrameRDD(part1.union(part2)).assignLabelToRows()
    }
    else {
      new EdgeFrameRDD(other.convertToNewSchema(unionedSchema)).assignLabelToRows()
    }
  }

  def toEdgeRDD: RDD[Edge] = {
    this.mapEdges(_.toEdge)
  }

  /**
   * Convert this EdgeFrameRDD to a GB Edge RDD
   */
  def toGbEdgeRDD: RDD[GBEdge] = {
    this.mapEdges(_.toGbEdge)
  }

}
