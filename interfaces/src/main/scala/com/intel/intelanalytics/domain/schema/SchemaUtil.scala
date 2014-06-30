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

object SchemaUtil {
  /**
   * Resolve naming conflicts when both left and right side of join operation have same column names
   * @param leftColumns columns for the left side of join operation
   * @param rightColumns columns for the right side of join operation
   * @return
   */
  def resolveSchemaNamingConflicts(leftColumns: List[(String, DataType)], rightColumns: List[(String, DataType)]): List[(String, DataType)] = {

    val funcAppendLetterForConflictingNames = (left: List[(String, DataType)], right: List[(String, DataType)], appendLetter: String) => {

      var leftColumnNames = left.map(r => r._1)

      left.map(r =>
        if (right.map(i => i._1).contains(r._1)) {
          var name = r._1 + "_" + appendLetter
          while (leftColumnNames.contains(name)) {
            name = name + "_" + appendLetter
          }
          leftColumnNames = leftColumnNames ++ List(name)
          (name, r._2)
        }
        else
          r)
    }

    val left = funcAppendLetterForConflictingNames(leftColumns, rightColumns, "L")
    val right = funcAppendLetterForConflictingNames(rightColumns, leftColumns, "R")

    left ++ right
  }

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

    val oldNames = oldSchema.columns.map(_._1).toArray
    newSchema.columns.toArray.map {
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

  /**
   * Merge schema for the purpose of appending two datasets.
   * @param originalSchema Schema of the original DataFrame
   * @return a single Schema with columns from both using the ordering of the originalSchema
   */
  def mergeSchema(originalSchema: Schema, appendedSchema: Schema): Schema = {
    if (originalSchema == appendedSchema)
      originalSchema
    else {
      val appendedColumns = originalSchema.columns ++ appendedSchema.columns
      val columnOrdering: List[String] = appendedColumns.map { case (name, dataTypes) => name }.distinct
      val groupedColumns = appendedColumns.groupBy { case (name, dataTypes) => name }

      val newColumns = columnOrdering.map(key => {
        (key, DataTypes.mergeTypes(groupedColumns(key).map { case (name, dataTypes) => dataTypes }))
      })

      Schema(newColumns)
    }
  }
}
