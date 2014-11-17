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
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, sql }
import org.apache.spark.sql.catalyst.expressions.{ AttributeReference, GenericMutableRow }
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{ ExistingRdd, SparkLogicalPlan }
import org.apache.spark.sql.{ SQLContext, SchemaRDD }
import SparkContext._

import scala.reflect.ClassTag

/**
 * A Frame RDD is a SchemaRDD with our version of the associated schema.
 *
 * This is our preferred format for loading frames as RDDs.
 *
 * @param frameSchema  the schema describing the columns of this frame
 * @param sqlContext a spark SQLContext
 * @param logicalPlan a logical plan describing the SchemaRDD
 */
class FrameRDD(val frameSchema: Schema,
               sqlContext: SQLContext,
               logicalPlan: LogicalPlan)
    extends SchemaRDD(sqlContext, logicalPlan) {

  def this(schema: Schema, rowRDD: RDD[sql.Row]) = this(schema, new SQLContext(rowRDD.context), FrameRDD.createLogicalPlanFromSql(schema, rowRDD))

  /**
   * A Frame RDD is a SchemaRDD with our version of the associated schema
   *
   * @param schema  the schema describing the columns of this frame
   * @param schemaRDD an existing schemaRDD that this FrameRDD will represent
   */
  def this(schema: Schema, schemaRDD: SchemaRDD) = this(schema, schemaRDD.sqlContext, schemaRDD.queryExecution.logical)

  /** This wrapper provides richer API for working with Rows */
  val rowWrapper = new RowWrapper(frameSchema)

  /**
   * Convert this FrameRDD into a LegacyFrameRDD of type RDD[Array[Any]]
   */
  def toLegacyFrameRDD: LegacyFrameRDD = {
    new LegacyFrameRDD(this.frameSchema, this.baseSchemaRDD)
  }

  /**
   * Convert FrameRDD into RDD[LabeledPoint] format required by MLLib
   */
  def toLabeledPointRDD(labelColumnName: String, featureColumnNames: List[String]): RDD[LabeledPoint] = {
    this.mapRows(row =>
      {
        val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
        new LabeledPoint(DataTypes.toDouble(row.value(labelColumnName)), new DenseVector(features.toArray))
      })
  }

  /**
   * Spark map with a rowWrapper
   */
  def mapRows[U: ClassTag](mapFunction: (RowWrapper) => U): RDD[U] = {
    this.map(sqlRow => {
      mapFunction(rowWrapper(sqlRow))
    })
  }

  /**
   * Spark groupBy with a rowWrapper
   */
  def groupByRows[K: ClassTag](function: (RowWrapper) => K): RDD[(K, scala.Iterable[sql.Row])] = {
    this.groupBy(row => {
      function(rowWrapper(row))
    })
  }

  /**
   * Create a new FrameRDD that is only a subset of the columns of this FrameRDD
   * @param columnNames names to include
   * @return the FrameRDD with only some columns
   */
  def selectColumns(columnNames: List[String]): FrameRDD = {
    if (columnNames.isEmpty) {
      throw new IllegalArgumentException("list of column names can't be empty")
    }
    new FrameRDD(frameSchema.copySubset(columnNames), mapRows(row => row.valuesAsRow(columnNames)))
  }

  /**
   * Drop columns - create a new FrameRDD with the columns specified removed
   */
  def dropColumns(columnNames: List[String]): FrameRDD = {
    convertToNewSchema(frameSchema.dropColumns(columnNames))
  }

  /**
   * Drop all columns with the 'ignore' data type.
   *
   * The ignore data type is a slight hack for ignoring some columns on import.
   */
  def dropIgnoreColumns(): FrameRDD = {
    convertToNewSchema(frameSchema.dropIgnoreColumns())
  }

  /**
   * Union two Frame's, merging schemas if needed
   */
  def union(other: FrameRDD): FrameRDD = {
    val unionedSchema = frameSchema.union(other.frameSchema)
    val part1 = convertToNewSchema(unionedSchema)
    val part2 = other.convertToNewSchema(unionedSchema)
    val unionedRdd = part1.toSchemaRDD.union(part2)
    new FrameRDD(unionedSchema, unionedRdd)
  }

  def renameColumn(oldName: String, newName: String): FrameRDD = {
    new FrameRDD(frameSchema.renameColumn(oldName, newName), this)
  }

  /**
   * Add/drop columns to make this frame match the supplied schema
   *
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  def convertToNewSchema(updatedSchema: Schema): FrameRDD = {
    if (frameSchema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new FrameRDD(updatedSchema, mapRows(row => row.valuesForSchema(updatedSchema)))
    }
  }

  /**
   * Sort by one or more columns
   * @param columnNamesAndAscending column names to sort by, true for ascending, false for descending
   * @return the sorted Frame
   */
  def sortByColumns(columnNamesAndAscending: List[(String, Boolean)]): FrameRDD = {
    require(columnNamesAndAscending != null && columnNamesAndAscending.length > 0, "one or more columnNames is required")

    val columnNames = columnNamesAndAscending.map(_._1)
    val ascendingPerColumn = columnNamesAndAscending.map(_._2)
    val pairRdd = mapRows(row => (row.values(columnNames), row.data))

    implicit val multiColumnOrdering = new Ordering[List[Any]] {
      override def compare(a: List[Any], b: List[Any]): Int = {
        for (i <- 0 to a.length - 1) {
          val columnA = a(i)
          val columnB = b(i)
          val result = DataTypes.compare(columnA, columnB)
          if (result != 0) {
            if (ascendingPerColumn(i)) {
              // ascending
              return result
            }
            else {
              // descending
              return result * -1
            }
          }
        }
        0
      }
    }

    // ascending is always true here because we control in the ordering
    val sortedRows = pairRdd.sortByKey(ascending = true).values
    new FrameRDD(frameSchema, sortedRows)
  }

  /**
   * Create a new RDD where unique ids are assigned to a specified value.
   * The ids will be long values that start from a specified value.
   * The ids are inserted into a specified column. if it does not exist the column will be created.
   *
   * (used for _vid and _eid in graphs but can be used elsewhere)
   *
   * @param columnName column to insert ids into (adding if needed)
   * @param startId  the first id to add (defaults to 0), incrementing from there
   */
  def assignUniqueIds(columnName: String, startId: Long = 0): FrameRDD = {
    val sumsAndCounts = MiscFrameFunctions.getPerPartitionCountAndAccumulatedSum(this)

    val newRows: RDD[sql.Row] = this.mapPartitionsWithIndex((i, rows) => {
      val (ct: Int, sum: Int) = if (i == 0) (0, 0)
      else sumsAndCounts(i - 1)
      val partitionStart = sum + startId
      rows.zipWithIndex.map {
        case (row, index) => {
          val id: Long = partitionStart + index
          rowWrapper(row).addOrSetValue(columnName, id)
        }
      }
    })

    val newSchema: Schema = if (!frameSchema.hasColumn(columnName)) {
      frameSchema.addColumn(columnName, DataTypes.int64)
    }
    else
      frameSchema

    new FrameRDD(newSchema, this.sqlContext, FrameRDD.createLogicalPlanFromSql(newSchema, newRows))
  }

}

/**
 * Static Methods for FrameRDD mostly deals with
 */
object FrameRDD {

  def toFrameRDD(schema: Schema, rowRDD: RDD[Row]) = {
    new FrameRDD(schema, new SQLContext(rowRDD.context), FrameRDD.createLogicalPlan(schema, rowRDD))
  }

  /**
   * Creates a logical plan for creating a SchemaRDD from an existing Row object and our schema representation
   * @param schema Schema describing the rdd
   * @param rows the RDD containing the data
   * @return A SchemaRDD with a schema corresponding to the schema object
   */
  def createLogicalPlan(schema: Schema, rows: RDD[Array[Any]]): LogicalPlan = {
    val rdd = FrameRDD.toRowRDD(schema, rows)
    createLogicalPlanFromSql(schema, rdd)
  }

  /**
   * Creates a logical plan for creating a SchemaRDD from an existing sql.Row object and our schema representation
   * @param schema Schema describing the rdd
   * @param rows RDD[org.apache.spark.sql.Row] containing the data
   * @return A SchemaRDD with a schema corresponding to the schema object
   */
  def createLogicalPlanFromSql(schema: Schema, rows: RDD[sql.Row]): LogicalPlan = {
    val structType = this.schemaToStructType(schema.columnTuples)
    val attributes = structType.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())

    SparkLogicalPlan(ExistingRdd(attributes, rows))(new SQLContext(rows.context))
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
              val colType = schema.columnTuples(i)._2
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
          case x if x.equals(DataTypes.ignore) => StringType
        }, nullable = true)
    }
    StructType(fields)
  }
}
