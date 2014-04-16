package com.intel.graphbuilder.parser

import org.apache.commons.lang3.StringUtils

/**
 * Column Definition including the Index of the Column
 *
 * @param columnName the name of the column
 * @param dataType the dataType of the column (needed to infer the schema from the parsing rules)
 * @param columnIndex the index position of the column (if null, index will be inferred from the column order)
 */
case class ColumnDef(columnName: String, dataType: Class[_], columnIndex: Integer) {

  if (StringUtils.isEmpty(columnName)) {
    throw new IllegalArgumentException("columnName cannot be empty")
  }
  if (columnIndex != null && columnIndex < 0) {
    throw new IllegalArgumentException("columnIndex needs to be greater than or equal to zero")
  }

  /**
   * Index will be assumed based on the order of the columns
   * @param columnName the name of the column
   * @param dataType the dataType of the column (needed to infer the schema from the parsing rules)
   */
  def this(columnName: String, dataType: Class[_]) {
    this(columnName, dataType, null)
  }
}


/**
 * Defines the schema of the rows of input
 * @param columns the definitions for each column
 */
case class InputSchema(columns: Seq[ColumnDef]) extends Serializable {

  /**
   * Convenience constructor for creating InputSchemas when all of the columns are the same type
   * @param columnNames the column names as the appear in order
   * @param columnType the dataType shared by all columns (needed to infer the schema from the parsing rules)
   */
  def this(columnNames: Seq[String], columnType: Class[_]) {
    this(columnNames.map(columnName => new ColumnDef(columnName, columnType)))
  }

  private lazy val schema = {
    var schema = Map[String, ColumnDef]()
    var columnIndex = 0
    for (column <- columns) {
      schema = schema + (column.columnName -> {
        if (column.columnIndex == null) column.copy(columnIndex = columnIndex)
        else column
      })
      columnIndex += 1
    }
    schema
  }

  /**
   * Lookup the index of a column by name
   */
  def columnIndex(columnName: String): Int = {
    schema(columnName).columnIndex
  }

  /**
   * Lookup the type of a column by name
   */
  def columnType(columnName: String): Class[_] = {
    schema(columnName).dataType
  }

  /**
   * Number of columns in the schema
   */
  def size: Int = {
    schema.size
  }
}
