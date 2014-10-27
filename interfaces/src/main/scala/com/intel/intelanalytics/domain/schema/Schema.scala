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

package com.intel.intelanalytics.domain.schema

import com.intel.intelanalytics.domain.schema.DataTypes.DataType

import scala.collection.parallel.mutable

/**
 * Column - this is a nicer wrapper for columns than just tuples
 *
 * @param name the column name
 * @param dataType the type
 * @param index Columns can track their own indices once they are added to a schema, -1 if not defined
 *              Normally only the Schema would set the index (except in unit tests)
 *              (Not sure if this is a good idea or not.  I saw some plugins passing a name and index
 *              everywhere so it seemed better to encapsulate it)
 */
case class Column(name: String, dataType: DataType, var index: Int = -1)

/**
 * Extra schema if this is a vertex frame
 */
case class VertexSchema(label: String, idColumnName: Option[String] = None) {

  /**
   * If the id column name had already been defined, use that name, otherwise use the supplied name
   * @param nameIfNotAlreadyDefined name to use if not defined
   * @return the name to use
   */
  def determineIdColumnName(nameIfNotAlreadyDefined: String): String = {
    idColumnName.getOrElse(nameIfNotAlreadyDefined)
  }

}

/**
 * Extra schema if this is an edge frame
 * @param label the label for this edge list
 * @param srcVertexLabel the src "type" of vertices this edge connects
 * @param destVertexLabel the destination "type" of vertices this edge connects
 * @param directed true if edges are directed, false if they are undirected
 */
case class EdgeSchema(label: String, srcVertexLabel: String, destVertexLabel: String, directed: Boolean = false)

/**
 * Schema for a data frame. Contains the columns with names and data types.
 * @param columns the columns in the data frame
 */
case class Schema(columns: List[Column] = List[Column](),
                  vertexSchema: Option[VertexSchema] = None,
                  edgeSchema: Option[EdgeSchema] = None) {

  /**
   * Legacy signature
   *
   * Schema was defined previously as a list of tuples.  This constructor was introduced to so
   * all of the dependent code wouldn't need to be changed.
   *
   * @param columnTuples columns as tuples
   */
  def this(columnTuples: List[(String, DataType)]) = {
    this(columnTuples.map { case (name, dataType) => Column(name, dataType) }, None, None)
  }

  require(columns != null, "columns must not be null")
  require(vertexSchema != null, "vertexSchema must not be null")
  require(edgeSchema != null, "edgeSchema must not be null")
  if (vertexSchema.isDefined) {
    require(hasColumnWithType("_vid", DataTypes.int64), "schema did not have int64 _vid column: " + columns)
    require(hasColumnWithType("_label", DataTypes.str), "schema did not have string _label column: " + columns)
    if (vertexSchema.get.idColumnName != null) {
      //require(hasColumn(vertexSchema.get.idColumnName), s"schema must contain vertex id column ${vertexSchema.get.idColumnName}")
    }
  }
  else if (edgeSchema.isDefined) {
    require(hasColumnWithType("_eid", DataTypes.int64), "schema did not have int64 _eid column: " + columns)
    require(hasColumnWithType("_src_vid", DataTypes.int64), "schema did not have int64 _src_vid column: " + columns)
    require(hasColumnWithType("_dest_vid", DataTypes.int64), "schema did not have int64 _dest_vid column: " + columns)
    require(hasColumnWithType("_label", DataTypes.str), "schema did not have string _label column: " + columns)
  }

  // assign indices
  columns.zipWithIndex.foreach { case (column, index) => column.index = index }

  /**
   * Map of names to columns
   */
  private lazy val namesToColumns = columns.map(col => (col.name, col)).toMap

  /**
   * Label column if this is an edge or vertex frame, None otherwise
   */
  def label: Option[String] = {
    if (vertexSchema.isDefined) {
      Some(vertexSchema.get.label)
    }
    else if (edgeSchema.isDefined) {
      Some(edgeSchema.get.label)
    }
    else {
      None
    }
  }

  def columnNames: List[String] = {
    namesToColumns.keys.toList
  }

  /**
   * True if this schema contains the supplied columnName
   */
  def hasColumn(columnName: String): Boolean = {
    namesToColumns.contains(columnName)
  }

  /**
   * True if this schema contains all of the supplied columnNames
   */
  def hasColumns(columnNames: Seq[String]): Boolean = {
    columnNames.forall(hasColumn)
  }

  /**
   * True if this schema contains the supplied columnName with the given dataType
   */
  def hasColumnWithType(columnName: String, dataType: DataType): Boolean = {
    hasColumn(columnName) && column(columnName).dataType == dataType
  }

  /**
   * Validate that the list of column names provided exist in this schema
   * throwing an exception if any does not exist.
   */
  def validateColumnsExist(columnNames: Iterable[String]): Unit = {
    columnNames.foreach(columnName =>
      if (!hasColumn(columnName)) {
        throw new IllegalArgumentException(s"Column name $columnName was not found in schema " + columnNamesAsString)
      }
    )

  }

  /**
   * Column names as comma separated list in a single string
   * (useful for error messages, etc)
   */
  def columnNamesAsString: String = {
    columnNames.mkString(", ")
  }

  // TODO: add a rename column method, since renaming columns shows up in Edge and Vertex schema it is more complicated

  /**
   * get column index by column name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = {
    val index = columns.indexWhere(column => column.name == columnName)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid column name $columnName provided, please choose from: " + columnNamesAsString)
    else
      index
  }

  /**
   * Retrieve list of column index based on column names
   * @param columnNames input column names
   */
  def columnIndices(columnNames: Seq[String]): Seq[Int] = {
    if (columnNames.isEmpty)
      (0 to (columns.length - 1)).toList
    else {
      columnNames.map(columnName => columnIndex(columnName))
    }
  }

  /**
   * Copy a subset of columns into a new Schema
   * @param columnNames the columns to keep
   * @return the new Schema
   */
  def copySubset(columnNames: Seq[String]): Schema = {
    val indices = columnIndices(columnNames)
    val columnSubset = indices.map(i => columns(i)).toList
    Schema(columnSubset, vertexSchema, edgeSchema)
  }

  /**
   * Union schemas together, keeping as much info as possible.
   *
   * Vertex and/or Edge schema information will be maintained for this schema only
   *
   * Column type conflicts will cause error
   */
  def union(schema: Schema): Schema = {
    // check for conflicts
    for (columnName <- schema.columnNames) {
      if (hasColumn(columnName)) {
        require(hasColumnWithType(columnName, schema.columnDataType(columnName)), s"columns with same name $columnName didn't have matching types")
      }
    }
    val combinedColumns = (this.namesToColumns ++ schema.namesToColumns).values.toList
    Schema(combinedColumns, vertexSchema, edgeSchema)
  }

  /**
   * get column datatype by column name
   * @param columnName name of the column
   */
  def columnDataType(columnName: String): DataType = {
    column(columnName).dataType
  }

  /**
   * Get all of the info about a column - this is a nicer wrapper than tuples
   *
   * @param columnName the name of the column
   * @return complete column info
   */
  def column(columnName: String): Column = {
    namesToColumns.getOrElse(columnName, throw new IllegalArgumentException(s"No column named $columnName"))
  }

  /**
   * Convenience method for optionally getting a Column.
   *
   * This is helpful when specifying a columnName was optional for the user.
   *
   * @param columnName the name of the column
   * @return complete column info, if a name was provided
   */
  def column(columnName: Option[String]): Option[Column] = {
    columnName match {
      case Some(name) => Some(column(name))
      case None => None
    }
  }

  /**
   * Get all of the info about a column - this is a nicer wrapper than tuples
   *
   * @param columnIndex the index for the column
   * @return complete column info
   */
  def column(columnIndex: Int): Column = columns(columnIndex)

  /**
   * Add a column to the schema
   * @param columnName name
   * @param dataType the type for the column
   * @return a new copy of the Schema with the column added
   */
  def addColumn(columnName: String, dataType: DataType): Schema = {
    if (columnNames.contains(columnName)) {
      throw new IllegalArgumentException(s"Cannot add a duplicate column name: $columnName")
    }
    copy(columns = columns :+ Column(columnName, dataType))
  }

  /**
   * Remove a column from this schema
   * @param columnName the name to remove
   * @return a new copy of the Schema with the column removed
   */
  def dropColumn(columnName: String): Schema = {
    copy(columns = columns.drop(columnIndex(columnName) + 1))
  }

  /**
   * Remove a list of columns from this schema
   * @param columnNames the names to remove
   * @return a new copy of the Schema with the columns removed
   */
  def dropColumns(columnNames: List[String]): Schema = {
    var newSchema = this
    if (columnNames != null) {
      columnNames.foreach(columnName =>
        newSchema = newSchema.dropColumn(columnName)
      )
    }
    newSchema
  }

  /**
   * Remove columns by the indices
   * @param columnIndices the indices to remove
   * @return a new copy of the Schema with the columns removed
   */
  def dropColumnsByIndex(columnIndices: Seq[Int]): Schema = {
    val remainingColumns = {
      columnIndices match {
        case singleColumn if singleColumn.length == 1 =>
          columnTuples.take(singleColumn(0)) ++ columnTuples.drop(singleColumn(0) + 1)
        case _ =>
          columnTuples.zipWithIndex.filter(elem => !columnIndices.contains(elem._2)).map(_._1)
      }
    }
    legacyCopy(remainingColumns)
  }

  /**
   * Convert data type for a column
   * @param columnName the column to change
   * @param updatedDataType the new data type for that column
   * @return the updated Schema
   */
  def convertType(columnName: String, updatedDataType: DataType): Schema = {
    val col = column(columnName)
    copy(columns = columns.updated(col.index, col.copy(dataType = updatedDataType)))
  }

  /**
   * Rename a column
   * @param existingName the old name
   * @param newName the new name
   * @return the updated schema
   */
  def renameColumn(existingName: String, newName: String): Schema = {
    copy(columns = columns.updated(columnIndex(existingName), column(existingName).copy(name = newName)))
  }

  /**
   * Re-order the columns in the schema.
   *
   * No columns will be dropped.  Any column not named will be tacked onto the end.
   *
   * @param columnNames the names you want to occur first, in the order you want
   * @return the updated schema
   */
  def reorderColumns(columnNames: List[String]): Schema = {
    validateColumnsExist(columnNames)
    val reorderedColumns = columnNames.map(name => column(name))
    val additionalColumns = columns.filterNot(column => columnNames.contains(column.name))
    copy(columns = reorderedColumns ++ additionalColumns)
  }

  /**
   * Get the list of columns except those provided
   * @param columnNamesToExclude columns you want to filter
   * @return the other columns, if any
   */
  def columnsExcept(columnNamesToExclude: List[String]): List[Column] = {
    dropColumns(columnNamesToExclude).columns
  }

  /**
   * Legacy column format for schemas
   *
   * Schema was defined previously as a list of tuples.  This method was introduced to so
   * all of the dependent code wouldn't need to be changed.
   *
   * @deprecated legacy use only - use nicer API instead
   */
  def columnTuples: List[(String, DataType)] = {
    columns.map(column => (column.name, column.dataType))
  }

  /**
   * Legacy copy() method
   *
   * Schema was defined previously as a list of tuples.  This method was introduced to so
   * all of the dependent code wouldn't need to be changed.
   *
   * @deprecated don't use - legacy support only
   */
  def legacyCopy(columnTuples: List[(String, DataType)]): Schema = {
    val updated = columnTuples.map { case (name, dataType) => Column(name, dataType) }
    copy(columns = updated)
  }
}

