/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.domain.schema

import com.intel.intelanalytics.domain.schema.DataTypes.DataType

/**
 * TODO: we should move this implementation to Schema class or be using implementation that is already there
 */
@deprecated("use Schema instead")
object SchemaUtil {

  // TODO: remove hard coded strings

  /**
   * Schema for Error Frames
   */
  val ErrorFrameSchema = new FrameSchema(List(Column("original_row", DataTypes.str), Column("error_message", DataTypes.str)))

  /**
   * Convert a row of values from one schema to another while maintaining the order specified in newColumns.
   * if a row does not exist in the old schema set the value to null
   *
   * @param oldSchema The columns found in the original schema
   * @param newSchema The columns found in the new schema
   * @param row an array of values matching the original schema
   * @return an array of values matching the new schema
   */
  def convertSchema(oldSchema: Schema, newSchema: Schema, row: Array[_ <: Any]): Array[Any] = {

    val oldNames = oldSchema.columnTuples.map(_._1).toArray
    newSchema.columnTuples.toArray.map {
      case ((name, columnType)) => {
        val index = oldNames.indexOf(name)
        if (index != -1) {
          val value = row(index)
          if (value != null)
            columnType.parse(value.toString).get
          else
            null
        }
        else
          null
      }
    }
  }

  // TODO: use implementation in Schema

  /**
   * Merge schema for the purpose of appending two datasets.
   * @param originalSchema Schema of the original DataFrame
   * @return a single Schema with columns from both using the ordering of the originalSchema
   */
  @deprecated("use union() implementation in Schema")
  def mergeSchema(originalSchema: Schema, appendedSchema: Schema): Schema = {
    if (originalSchema == appendedSchema)
      originalSchema
    else {
      val appendedColumns = originalSchema.columnTuples ++ appendedSchema.columnTuples
      val columnOrdering: List[String] = appendedColumns.map { case (name, dataTypes) => name }.distinct
      val groupedColumns = appendedColumns.groupBy { case (name, dataTypes) => name }

      val newColumns = columnOrdering.map(key => {
        Column(key, DataTypes.mergeTypes(groupedColumns(key).map { case (name, dataTypes) => dataTypes }))
      })

      originalSchema.copy(newColumns)
    }
  }

}
