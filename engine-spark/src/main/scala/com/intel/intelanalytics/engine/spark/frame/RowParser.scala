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

import util.parsing.combinator.RegexParsers
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes

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
class RowParser(separator: Char, columnTypes: Array[DataType]) extends RegexParsers with Serializable {

  override def skipWhitespace = false
  private def space = regex("[ \\n]*".r)
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

  /**
   * Apply method parses the string and returns a list of String tokens
   * @param line to be parsed
   */
  def splitLineIntoParts(line: String): Array[String] = parseAll(record, line) match {
    case Success(result, _) => result.toArray
    case failure: NoSuccess => { throw new Exception("Parse Failed") }
  }

  def record = repsep(mainToken, separator.toString)
  def mainToken = spaceWithSingleQuotes | spaceWithQuotes | doubleQuotes | singleQuotes | unquotes | empty
  /** function to evaluate empty fields*/
  lazy val empty = success("")
  /** function to evaluate single quotes*/
  lazy val singleQuotes = "'" ~> "[^']+".r <~ "'"
  /** function to evaluate double quotes*/
  lazy val doubleQuotes: Parser[String] = "\"" ~> "[^\"]+".r <~ "\""
  /** function to evaluate normal tokens*/
  lazy val unquotes = ("[^" + separator + "]+").r
  /** function to evaluate normal tokens*/
  lazy val spaceWithSingleQuotes = space ~> "'" ~> "[^']+".r <~ "'" <~ space
  /** function to evaluate normal tokens*/
  lazy val spaceWithQuotes = space ~> '"' ~> "[^\"]+".r <~ '"' <~ space

}