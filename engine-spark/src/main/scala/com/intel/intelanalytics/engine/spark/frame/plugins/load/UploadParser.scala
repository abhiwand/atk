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

package com.intel.intelanalytics.engine.spark.frame.plugins.load

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import org.apache.commons.csv.{ CSVFormat, CSVParser }

import scala.collection.JavaConversions.asScalaIterator

class UploadParser(columnTypes: Array[DataType]) extends Serializable {

  val converter = DataTypes.parseMany(columnTypes)(_)

  /**
   * Parse a line into a RowParseResult
   * @param row a single line
   * @return the result - either a success row or an error row
   */
  def apply(row: List[Any]): RowParseResult = {
    try {
      RowParseResult(parseSuccess = true, converter(row.toArray))
    }
    catch {
      case e: Exception =>
        RowParseResult(parseSuccess = false, Array(row.mkString(","), e.toString))
    }
  }
}