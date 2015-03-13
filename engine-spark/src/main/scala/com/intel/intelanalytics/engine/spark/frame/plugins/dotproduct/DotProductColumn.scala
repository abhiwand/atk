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

package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import com.intel.intelanalytics.domain.schema.{ Column, DataTypes }
import org.apache.spark.frame.FrameRdd

/**
 * Columns and expected size of column.
 *
 * @param column Column
 * @param expectedSize Column size is one for scalars, and the number of elements in list for de-limited strings
 */
case class DotProductColumn(column: Column, expectedSize: Int)

object DotProductColumn extends Serializable {

  /**
   * Creates columns used by dot-product which stores the column and expected size
   *
   * @param frameRdd Data frame
   * @param columnName Column name
   * @return Dot-product plugin
   */
  def createDotProductColumn(frameRdd: FrameRdd, columnName: String): DotProductColumn = {
    val column = frameRdd.frameSchema.column(columnName)
    val columnSize = column.dataType match {
      case DataTypes.string => {
        //List of doubles represented as delimited string
        //Determine size of column based on first non-empty column value
        val firstDefinedValue = frameRdd.mapRows(row => row.value(columnName)).filter(_ != null).take(1)
        if (firstDefinedValue.nonEmpty) firstDefinedValue(0).toString.split(",").size else 1
      }
      case _ => 1
    }
    DotProductColumn(column, columnSize)
  }
}