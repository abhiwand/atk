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
