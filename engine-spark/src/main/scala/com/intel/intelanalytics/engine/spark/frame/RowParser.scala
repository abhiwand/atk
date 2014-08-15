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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes
import org.apache.commons.csv.{CSVParser, CSVFormat}
import scala.collection.JavaConversions.asScalaIterator

/**
 * Split a string based on delimiter into List[String]
 * <p>
 * Usage:
 * scala> import com.intelanalytics.engine.Row
 * scala> val out = RowParser.apply("foo,bar")
 * scala> val out = RowParser.apply("a,b,\"foo,is this ,bar\",foobar ")
 * scala> val out = RowParser.apply(" a,b,'',\"\"  ")
 * </p>
 *
 * @param separator delimiter character
 */
class RowParser(separator: Char, columnTypes: Array[DataType]) extends Serializable {

  val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)
    .withIgnoreSurroundingSpaces(true)

  val converter = DataTypes.parseMany(columnTypes)(_)

  /**
   * Parse a line into a RowParseResult
   * @param line a single line
   * @return the result - either a success row or an error row
   */
  def apply(line: String): RowParseResult = {
    try {
        val parts = splitLineIntoParts(line)
        RowParseResult(parseSuccess = true, converter(parts))
    }
    catch {
      case e: Exception =>
         RowParseResult(parseSuccess = false, Array(line, e.toString))
    }
  }

  private[frame] def splitLineIntoParts(line: String): Array[String] = {
    val records = CSVParser.parse(line, csvFormat).getRecords
    if (records.isEmpty) {
      Array.empty[String]
    }
    else {
      records.get(0).iterator().toArray
    }
  }

}