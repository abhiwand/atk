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