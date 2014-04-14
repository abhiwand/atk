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

package com.intel.intelanalytics.engine

/** This object parses comma delimited strings into List[String]
  * Usage:
  * scala> import com.intelanalytics.engine.Row
  * scala> val out = Row.parseRecord("foo,bar")
  * scala> val out = Row.parseRecord("a,b,\"foo,is this ,bar\",foobar ")
  * scala> val out = Row.parseRecord(" a,b,'',\"\"  ")
  * Modified from: http://poundblog.wordpress.com/2013/06/06/a-scala-parser-combinator-grammar-for-csv/
  */
import util.parsing.combinator.RegexParsers

trait RecordParser extends RegexParsers {
  /** A generic interface to handle parsing logic*/
  override val whiteSpace = """[ \t]""".r
  
  override val skipWhitespace = false
  
  /** Takes in the string and applies the parsing logic   */
  def apply(line: String) : List[String]={
  	parseAll(record,line).get
  }

  
  def record:Parser[List[String]] = repsep(field,",") ^^ {case x => x}
  
  // Specify various portions of strings and how to handle them and the order of precedence
  def field: Parser[String] = TEXT ||| SINGLEQUOTES | STRING  |EMPTY
 
  val STRING: Parser[String] = whiteSpace.* ~> "\"" ~> rep("\"\"" | """[^\"]""".r) <~ "\"" <~ whiteSpace.* ^^ makeString
  val TEXT: Parser[String] = rep1("""[^,\n\r\"]""".r) ^^ makeText
  val EMPTY: Parser[String] = "" ^^ makeEmpty
 
  def SINGLEQUOTES = "'" ~> "[^']+".r <~ "'" ^^ {case a => (""/:a)(_+_)}
  def makeText: (List[String]) => String
  def makeString: (List[String]) => String
  def makeEmpty: (String) => String
}
 

 
trait RecordParserAction {
  /** Implementation of text,string and empty cases handler*/
  // no trimming of WhiteSpace
  def makeText = (text: List[String]) => text.mkString("")
  // remove embracing quotation marks
  // replace double quotes by single quotes
  def makeString = (string: List[String]) => string.mkString("").replaceAll("\"\"", "\"")
  
  // modify result of EMPTY token if required
  def makeEmpty = (string: String) => ""
}

 
object Row extends RecordParser with RecordParserAction {
  /** Creates a Row with given input String
    *
    * @param line to be parsed
    */
    def parseRecord(line:String):List[String]={
    	//println(apply(record))
     return apply(line)
  }  
}