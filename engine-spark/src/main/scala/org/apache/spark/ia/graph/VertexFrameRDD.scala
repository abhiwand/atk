//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.apache.spark.ia.graph

import com.intel.graphbuilder.elements.{ GBVertex }
import com.intel.intelanalytics.domain.schema.{ VertexSchema, GraphSchema, Schema }
import com.intel.intelanalytics.engine.spark.frame.{ MiscFrameFunctions }
import org.apache.spark.frame.FrameRDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{ Row, SQLContext }

import scala.reflect.ClassTag

/**
 * Vertex List for a "Seamless" Graph
 *
 * @param schema  the schema describing the columns of this frame
 * @param sqlContext a spark SQLContext
 * @param logicalPlan a logical plan describing the SchemaRDD
 */
class VertexFrameRDD(schema: VertexSchema,
                     sqlContext: SQLContext,
                     logicalPlan: LogicalPlan) extends FrameRDD(schema, sqlContext, logicalPlan) {

  def this(frameRDD: FrameRDD) = this(frameRDD.frameSchema.asInstanceOf[VertexSchema], frameRDD.sqlContext, frameRDD.logicalPlan)

  def this(schema: VertexSchema, frameRDD: FrameRDD) = this(schema, frameRDD.sqlContext, frameRDD.logicalPlan)

  def this(schema: VertexSchema, rowRDD: RDD[sql.Row]) = this(schema, new SQLContext(rowRDD.context), FrameRDD.createLogicalPlanFromSql(schema, rowRDD))

  /** Vertex wrapper provides richer API for working with Vertices */
  val vertexWrapper = new VertexWrapper(schema)

  /**
   * Merge duplicate Vertices, creating a new Vertex that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  // TODO: implement or delete
  //  def mergeDuplicates(): VertexFrameRDD = {
  //    new VertexFrameRDD(schema, groupVerticesById().mapValues(dups => dups.reduce((m1, m2) => vertex(m1).merge(m2))).values)
  //  }

  /**
   * Drop duplicates based on user defined id
   */
  def dropDuplicates(): VertexFrameRDD = {
    val pairRdd = map(row => MiscFrameFunctions.createKeyValuePairFromRow(row.toArray, schema.columnIndices(Seq(schema.idColumnName.getOrElse(throw new RuntimeException("Cannot drop duplicates is id column has not yet been defined")), schema.label))))
    val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByKey(pairRdd)
    new VertexFrameRDD(FrameRDD.toFrameRDD(schema, duplicatesRemoved))
  }

  def groupVerticesById() = {
    this.groupBy(data => vertexWrapper(data).idValue)
  }

  /**
   * Map over vertices
   * @param mapFunction map function that operates on a VertexWrapper
   * @tparam U return type that will be the in resulting RDD
   */
  def mapVertices[U: ClassTag](mapFunction: (VertexWrapper) => U): RDD[U] = {
    this.map(data => {
      mapFunction(vertexWrapper(data))
    })
  }

  /**
   * RDD of idColumn and _vid
   */
  def idColumns: RDD[(Any, Long)] = {
    mapVertices(vertex => (vertex.idValue, vertex.vid))
  }

  /**
   * Convert this RDD in match the schema provided
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  override def convertToNewSchema(updatedSchema: Schema): VertexFrameRDD = {
    if (schema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new VertexFrameRDD(super.convertToNewSchema(updatedSchema))
    }
  }

  def assignLabelToRows(): VertexFrameRDD = {
    new VertexFrameRDD(schema, mapVertices(vertex => vertex.setLabel(schema.label)))
  }

  /**
   * Append vertices to the current frame:
   * - overwriting existing vertices, if needed
   * - union the schemas to match, if needed
   * @param preferNewVertexData true to prefer new vertex data, false to prefer existing vertex data - during merge.
   *                            false is useful for createMissingVertices, otherwise you probably always want true.
   */
  def append(other: FrameRDD, preferNewVertexData: Boolean = true): VertexFrameRDD = {
    val unionedSchema = schema.union(other.frameSchema).reorderColumns(GraphSchema.vertexSystemColumnNames).asInstanceOf[VertexSchema]

    val part2 = new VertexFrameRDD(other.convertToNewSchema(unionedSchema)).mapVertices(vertex => (vertex.idValue, (vertex.data, preferNewVertexData)))

    // TODO: better way to check for empty?
    val appended = if (take(1).length > 0) {
      val part1 = convertToNewSchema(unionedSchema).mapVertices(vertex => (vertex.idValue, (vertex.data, !preferNewVertexData)))
      dropDuplicates(part1.union(part2))
    }
    else {
      dropDuplicates(part2)
    }
    new VertexFrameRDD(unionedSchema, appended).assignLabelToRows()
  }

  /**
   * Drop duplicates
   * @param vertexPairRDD a pair RDD of the format (uniqueId: Any, (row: Row, preferred: Boolean))
   * @return rows without duplicates
   */
  private def dropDuplicates(vertexPairRDD: RDD[(Any, (Row, Boolean))]): RDD[Row] = {

    // TODO: do we care about merging?
    vertexPairRDD.reduceByKey {
      case ((row1: Row, row1Preferred: Boolean), (row2: Row, row2Preferred: Boolean)) =>
        if (row1Preferred) {
          // prefer newer data
          (row1, row1Preferred)
        }
        else {
          // otherwise choose randomly
          (row2, row2Preferred)
        }
    }.values.map { case (row: Row, rowNew: Boolean) => row }
  }

  /**
   * Define the ID column name
   */
  def setIdColumnName(name: String): VertexFrameRDD = {
    val updatedVertexSchema = schema.copy(idColumnName = Some(name))
    new VertexFrameRDD(updatedVertexSchema, this)
  }

  def toVertexRDD: RDD[Vertex] = {
    this.mapVertices(_.toVertex)
  }

  /**
   * Convert this VertexFrameRDD to a GB Vertex RDD
   */
  def toGbVertexRDD: RDD[GBVertex] = {
    this.mapVertices(_.toGbVertex)
  }
}
