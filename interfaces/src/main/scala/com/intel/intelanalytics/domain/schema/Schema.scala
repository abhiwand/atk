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

/**
 * Schema for a data frame. Contains the columns with names and data types.
 * @param columns the columns in the data frame
 */
case class Schema(columns: List[(String, DataType)] = List[(String, DataType)]()) {
  require(columns != null)

  /**
   * get column index by column name
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = columnIndex(Seq(columnName))(0)

  /**
   * Retrieve list of column index based on column names
   * @param columnNames input column names
   */
  def columnIndex(columnNames: Seq[String]): Seq[Int] = {
    if(columnNames.isEmpty)
      (0 to (columns.length - 1)).toList
    else
      columnNames.map(col => columns.indexWhere(columnTuple => columnTuple._1 == col))
  }
}
