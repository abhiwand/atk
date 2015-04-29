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

package org.apache.spark.frame

import com.intel.graphbuilder.elements.GBVertex
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame.FrameMeta
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, MiscFrameFunctions, LegacyFrameRdd, RowWrapper }
import com.intel.intelanalytics.engine.spark.graph.plugins.exportfromtitan.VertexSchemaAggregator
import org.apache.spark.ia.graph.VertexWrapper
import org.apache.spark.mllib.linalg.{ Vectors, Vector, DenseVector }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{ NewHadoopPartition, RDD }
import org.apache.spark.{ Partition, SparkContext, sql }
import org.apache.spark.sql.catalyst.expressions.{ AttributeReference, GenericMutableRow }
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{ ExistingRdd, SparkLogicalPlan }
import org.apache.spark.sql.{ SQLContext, SchemaRDD }
import SparkContext._
import parquet.hadoop.ParquetInputSplit

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
class FrameRdd(val frameSchema: Schema,
               sqlContext: SQLContext,
               logicalPlan: LogicalPlan)
    extends SchemaRDD(sqlContext, logicalPlan) {

  def this(schema: Schema, rowRDD: RDD[sql.Row]) = this(schema, new SQLContext(rowRDD.context), FrameRdd.createLogicalPlanFromSql(schema, rowRDD))

  /**
   * A Frame RDD is a SchemaRDD with our version of the associated schema
   *
   * @param schema  the schema describing the columns of this frame
   * @param schemaRDD an existing schemaRDD that this FrameRdd will represent
   */
  def this(schema: Schema, schemaRDD: SchemaRDD) = this(schema, schemaRDD.sqlContext, schemaRDD.queryExecution.logical)

  /** This wrapper provides richer API for working with Rows */
  val rowWrapper = new RowWrapper(frameSchema)

  /**
   * Convert this FrameRdd into a LegacyFrameRdd of type RDD[Array[Any]]
   */
  def toLegacyFrameRdd: LegacyFrameRdd = {
    new LegacyFrameRdd(this.frameSchema, this.baseSchemaRDD)
  }

  /**
   * Convert FrameRdd into RDD[LabeledPoint] format required by MLLib
   */
  def toLabeledPointRDD(labelColumnName: String, featureColumnNames: List[String]): RDD[LabeledPoint] = {
    this.mapRows(row =>
      {
        val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
        new LabeledPoint(DataTypes.toDouble(row.value(labelColumnName)), new DenseVector(features.toArray))
      })
  }

  /**
   * overrides the default behavior so new partitions get created in the sorted order to maintain the data order for the
   * user
   *
   * @return an array of partitions
   */
  override def getPartitions(): Array[org.apache.spark.Partition] = {
    val partitions = super.getPartitions

    if (partitions.length > 0 && partitions(0).isInstanceOf[NewHadoopPartition]) {
      val sorted = partitions.toList.sortBy(partition => {
        val split = partition.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
        if (split.isInstanceOf[ParquetInputSplit]) {
          val uri = split.asInstanceOf[ParquetInputSplit].getPath.toUri
          val index = uri.getPath.lastIndexOf("/")
          val filename = uri.getPath.substring(index)
          val fileNumber = filename.replaceAll("[a-zA-Z.\\-/]+", "")
          fileNumber.toLong
        }
        else {
          partition.index
        }
      })
      sorted.zipWithIndex.map {
        case (p: Partition, i: Int) => {
          val hp = p.asInstanceOf[NewHadoopPartition]
          new NewHadoopPartition(id, i, hp.serializableHadoopSplit.value)
        }
      }.toArray
    }
    else {
      partitions
    }
  }

  /**
   * Convert FrameRdd into RDD[Vector] format required by MLLib
   */
  def toVectorDenseRDD(featureColumnNames: List[String]): RDD[Vector] = {
    this.mapRows(row => {
      val array = row.valuesAsArray(featureColumnNames, flattenInputs = true)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })
  }

  def toDenseVectorRDDWithWeights(featureColumnNames: List[String], columnWeights: List[Double]): RDD[Vector] = {
    require(columnWeights.length == featureColumnNames.length, "Length of columnWeights and featureColumnNames needs to be the same")
    this.mapRows(row => {
      val array = row.valuesAsArray(featureColumnNames).map(row => DataTypes.toDouble(row))
      val columnWeightsArray = columnWeights.toArray
      val doubles = array.zip(columnWeightsArray).map { case (x, y) => x * y }
      Vectors.dense(doubles)
    }
    )
  }

  def toVectorRDD(featureColumnNames: List[String]) = {
    this mapRows (row =>
      {
        val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
        Vectors.dense(features.toArray)
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

  def selectColumn(columnName: String): FrameRdd = {
    selectColumns(List(columnName))
  }

  /**
   * Spark keyBy with a rowWrapper
   */
  def keyByRows[K: ClassTag](function: (RowWrapper) => K): RDD[(K, sql.Row)] = {
    this.keyBy(row => {
      function(rowWrapper(row))
    })
  }

  /**
   * Create a new FrameRdd that is only a subset of the columns of this FrameRdd
   * @param columnNames names to include
   * @return the FrameRdd with only some columns
   */
  def selectColumns(columnNames: List[String]): FrameRdd = {
    if (columnNames.isEmpty) {
      throw new IllegalArgumentException("list of column names can't be empty")
    }
    new FrameRdd(frameSchema.copySubset(columnNames), mapRows(row => row.valuesAsRow(columnNames)))
  }

  /**
   * Select a subset of columns while renaming them
   * @param columnNamesWithRename map of old names to new names
   * @return the new FrameRdd
   */
  def selectColumnsWithRename(columnNamesWithRename: Map[String, String]): FrameRdd = {
    if (columnNamesWithRename.isEmpty) {
      throw new IllegalArgumentException("map of column names can't be empty")
    }
    val preservedOrderColumnNames = frameSchema.columnNames.filter(name => columnNamesWithRename.contains(name))
    new FrameRdd(frameSchema.copySubsetWithRename(columnNamesWithRename), mapRows(row => row.valuesAsRow(preservedOrderColumnNames)))
  }

  /* Please see documentation. Zip works if 2 SchemaRDDs have the same number of partitions
     and same number of elements in  each partition */
  def zipFrameRdd(frameRdd: FrameRdd): FrameRdd = {
    new FrameRdd(frameSchema.addColumns(frameRdd.frameSchema.columns), this.zip(frameRdd).map { case (a, b) => sql.Row.fromSeq(a ++ b) })
  }

  /**
   * Drop columns - create a new FrameRdd with the columns specified removed
   */
  def dropColumns(columnNames: List[String]): FrameRdd = {
    convertToNewSchema(frameSchema.dropColumns(columnNames))
  }

  /**
   * Drop all columns with the 'ignore' data type.
   *
   * The ignore data type is a slight hack for ignoring some columns on import.
   */
  def dropIgnoreColumns(): FrameRdd = {
    convertToNewSchema(frameSchema.dropIgnoreColumns())
  }

  /**
   * Union two Frame's, merging schemas if needed
   */
  def union(other: FrameRdd): FrameRdd = {
    val unionedSchema = frameSchema.union(other.frameSchema)
    val part1 = convertToNewSchema(unionedSchema)
    val part2 = other.convertToNewSchema(unionedSchema)
    val unionedRdd = part1.toSchemaRDD.union(part2)
    new FrameRdd(unionedSchema, unionedRdd)
  }

  def renameColumn(oldName: String, newName: String): FrameRdd = {
    new FrameRdd(frameSchema.renameColumn(oldName, newName), this)
  }

  /**
   * Add/drop columns to make this frame match the supplied schema
   *
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  def convertToNewSchema(updatedSchema: Schema): FrameRdd = {
    if (frameSchema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new FrameRdd(updatedSchema, mapRows(row => row.valuesForSchema(updatedSchema)))
    }
  }

  /**
   * Sort by one or more columns
   * @param columnNamesAndAscending column names to sort by, true for ascending, false for descending
   * @return the sorted Frame
   */
  def sortByColumns(columnNamesAndAscending: List[(String, Boolean)]): FrameRdd = {
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
    new FrameRdd(frameSchema, sortedRows)
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
  def assignUniqueIds(columnName: String, startId: Long = 0): FrameRdd = {
    val sumsAndCounts: Map[Int, (Long, Long)] = MiscFrameFunctions.getPerPartitionCountAndAccumulatedSum(this)

    val newRows: RDD[sql.Row] = this.mapPartitionsWithIndex((i, rows) => {
      val (ct: Long, sum: Long) = if (i == 0) (0L, 0L)
      else sumsAndCounts(i - 1)
      val partitionStart = sum + startId
      rows.zipWithIndex.map {
        case (row: sql.Row, index: Int) => {
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

    new FrameRdd(newSchema, this.sqlContext, FrameRdd.createLogicalPlanFromSql(newSchema, newRows))
  }

  /**
   * Convert Vertex or Edge Frames to plain data frames
   */
  def toPlainFrame(): FrameRdd = {
    new FrameRdd(frameSchema.toFrameSchema, this.toSchemaRDD)
  }

  /**
   * Save this RDD to disk or other store
   * @param absolutePath location to store
   * @param storageFormat "file/parquet", "file/sequence", etc.
   */
  def save(absolutePath: String, storageFormat: String = "file/parquet"): Unit = {
    storageFormat match {
      case "file/sequence" => toSchemaRDD.saveAsObjectFile(absolutePath)
      case "file/parquet" => toSchemaRDD.saveAsParquetFile(absolutePath)
      case format => throw new IllegalArgumentException(s"Unrecognized storage format: $format")
    }
  }

}

/**
 * Static Methods for FrameRdd mostly deals with
 */
object FrameRdd {

  def toFrameRdd(schema: Schema, rowRDD: RDD[Row]) = {
    new FrameRdd(schema, new SQLContext(rowRDD.context), FrameRdd.createLogicalPlan(schema, rowRDD))
  }

  /**
   * Creates a logical plan for creating a SchemaRDD from an existing Row object and our schema representation
   * @param schema Schema describing the rdd
   * @param rows the RDD containing the data
   * @return A SchemaRDD with a schema corresponding to the schema object
   */
  def createLogicalPlan(schema: Schema, rows: RDD[Array[Any]]): LogicalPlan = {
    val rdd = FrameRdd.toRowRDD(schema, rows)
    createLogicalPlanFromSql(schema, rdd)
  }

  /**
   * Convert an RDD of mixed vertex types into a map where the keys are labels and values are FrameRdd's
   * @param gbVertexRDD Graphbuilder Vertex RDD
   * @return  keys are labels and values are FrameRdd's
   */
  def toFrameRddMap(gbVertexRDD: RDD[GBVertex]): Map[String, FrameRdd] = {

    import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

    // make sure all of the vertices have a label
    val labeledVertices = gbVertexRDD.labelVertices(Nil)

    // figure out the schemas of the different vertex types
    val vertexSchemaAggregator = new VertexSchemaAggregator(Nil)
    val vertexSchemas = labeledVertices.aggregate(vertexSchemaAggregator.zeroValue)(vertexSchemaAggregator.seqOp, vertexSchemaAggregator.combOp).values

    vertexSchemas.map(schema => {
      val filteredVertices: RDD[GBVertex] = labeledVertices.filter(v => {
        v.getProperty(GraphSchema.labelProperty) match {
          case Some(p) => p.value == schema.label
          case _ => throw new RuntimeException(s"Vertex didn't have a label property $v")
        }
      })
      val vertexWrapper = new VertexWrapper(schema)
      val rows = filteredVertices.map(gbVertex => vertexWrapper.create(gbVertex))
      val frameRdd = new FrameRdd(schema.toFrameSchema, rows)

      (schema.label, frameRdd)
    }).toMap
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
   * Defines a VectorType "StructType" for SchemaRDDs
   */
  val VectorType = ArrayType(DoubleType, containsNull = false)

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
          case x if x.isVector => VectorType
          case x if x.equals(DataTypes.ignore) => StringType
        }, nullable = true)
    }
    StructType(fields)
  }

  /**
   * Converts the spark DataTypes to our schema Datatypes
   * @return our schema DataType
   */
  def sparkDataTypeToSchemaDataType(dataType: org.apache.spark.sql.catalyst.types.DataType): DataType = {
    val intType = IntegerType.getClass()
    val longType = LongType.getClass()
    val floatType = FloatType.getClass()
    val doubleType = DoubleType.getClass()
    val stringType = StringType.getClass()
    val dateType = DateType.getClass()
    val timeStampType = TimestampType.getClass()

    val a = dataType.getClass()
    a match {
      case `intType` => int32
      case `longType` => int64
      case `floatType` => float32
      case `doubleType` => float64
      case `stringType` => DataTypes.string
      case `dateType` => DataTypes.string
      case `timeStampType` => DataTypes.string
      case _ => throw new IllegalArgumentException(s"unsupported type $a")
    }
  }

}
