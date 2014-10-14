//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.Rows.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{ GenericMutableRow, AttributeReference }
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{ ExistingRdd, SparkLogicalPlan }
import org.apache.spark.sql.{ SQLContext, SchemaRDD }

/**
 * A Frame RDD is a SchemaRDD with our version of the associated schema.
 *
 * This is our preferred format for loading frames as RDDs.
 *
 * @param schema  the schema describing the columns of this frame
 * @param sqlContext a spark SQLContext
 * @param logicalPlan a logical plan describing the SchemaRDD
 */
class FrameRDD(val schema: Schema,
               sqlContext: SQLContext,
               logicalPlan: LogicalPlan)
    extends SchemaRDD(sqlContext, logicalPlan) {
  /**
   * A Frame RDD is a SchemaRDD with our version of the associated schema
   *
   * @param schema  the schema describing the columns of this frame
   * @param rowRDD  RDD of type Row that corresponds to the supplied schema
   */
  def this(schema: Schema, rowRDD: RDD[Row]) = this(schema, new SQLContext(rowRDD.context), FrameRDD.createLogicalPlan(schema, rowRDD))

  /**
   * A Frame RDD is a SchemaRDD with our version of the associated schema
   *
   * @param schema  the schema describing the columns of this frame
   * @param schemaRDD an existing schemaRDD that this FrameRDD will represent
   */
  def this(schema: Schema, schemaRDD: SchemaRDD) = this(schema, schemaRDD.sqlContext, schemaRDD.queryExecution.logical)

  def toLegacyFrameRDD: LegacyFrameRDD = {
    new LegacyFrameRDD(this.schema, this.baseSchemaRDD)
  }
}

/**
 * Static Methods for FrameRDD mostly deals with
 */
private[frame] object FrameRDD {
  /**
   * Creates a logical plan for creating a SchemaRDD from an existing Row object and our schema representation
   * @return A SchemaRDD with a schema corresponding to the schema object
   */
  def createLogicalPlan(schema: Schema, rows: RDD[Row]): LogicalPlan = {
    val rdd = FrameRDD.toRowRDD(schema, rows)
    val structType = this.schemaToStructType(schema.columns)
    val attributes = structType.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())

    SparkLogicalPlan(ExistingRdd(attributes, rdd))
  }

  /**
   * Converts row object from an RDD[Array[Any]] to an RDD[Product] so that it can be used to create a SchemaRDD
   * @return RDD[org.apache.spark.sql.Row] with values equal to row object
   */
  def toRowRDD(schema: Schema, rows: RDD[Row]): RDD[org.apache.spark.sql.Row] = {
    val rowRDD: RDD[org.apache.spark.sql.Row] = rows.map(row => {
      val mutableRow = new GenericMutableRow(row.length)
      row.zipWithIndex.map {
        case (o, i) => {
          o match {
            case null => null
            case _ => {
              val colType = schema.columns(i)._2
              val value = o.asInstanceOf[colType.ScalaType]
              mutableRow(i) = value
            }

          }
        }
      }
      mutableRow
    })
    rowRDD
  }

  /**
   * Converts the schema object to a StructType for use in creating a SchemaRDD
   * @return StructType with StructFields corresponding to the columns of the schema object
   */
  def schemaToStructType(columns: List[(String, DataType)]): StructType = {
    val fields: Seq[StructField] = columns.map {
      case (name, dataType) =>
        StructField(name.replaceAll("\\s", ""), dataType match {
          case x if x.equals(DataTypes.int32) => IntegerType
          case x if x.equals(DataTypes.int64) => LongType
          case x if x.equals(DataTypes.float32) => FloatType
          case x if x.equals(DataTypes.float64) => DoubleType
          case x if x.equals(DataTypes.string) => StringType
        }, nullable = true)
    }
    StructType(fields)
  }
}
