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

package com.intel.intelanalytics.domain.frame.load

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ SchemaUtil, FrameSchema, Schema }

/**
 * The case classes in this file are used to parse the json submitted as part of a load or append call
 */

/**
 * Object used for parsing and then executing the frame.append command
 *
 * @param destination DataFrame to load data into. Should be either a uri or id
 * @param source Object describing the data to load into the destination. Includes the Where and How of loading.
 */
case class Load(destination: FrameReference, source: LoadSource)

/**
 * Describes a resource that should be loaded into a DataFrame
 *
 * @param sourceType Source object that can be parsed into an RDD. Such as "file" or "frame"
 * @param uri Location of data to load. Should be appropriate for the source_type.
 * @param parser Object describing how to parse the resource. If data already an RDD can be set to None
 */
case class LoadSource(sourceType: String, uri: String, parser: Option[LineParser] = None, data: Option[List[List[Any]]] = None, startTag: Option[List[String]] = None, endTag: Option[List[String]] = None) {

  require(sourceType != null, "sourceType cannot be null")
  require(sourceType == "frame" || sourceType == "file" || sourceType == "strings" || sourceType == "linefile" || sourceType == "multilinefile" || sourceType == "xmlfile",
    "sourceType must be a valid type")
  require(uri != null, "uri cannot be null")
  require(parser != null, "parser cannot be null")
  if (sourceType == "frame" || sourceType == "file" || sourceType == "linefile" || sourceType == "multilinefile") {
    require(data == None, "if this is not a strings file the data must be None")
  }
  if (sourceType == "strings") {
    require(data != None, "if the sourceType is strings data must not be None")
  }
  if (sourceType == "multilinefile" || sourceType == "xmlfile") {
    require(startTag != None && endTag != None, "if this is a multiline file the start and end tags must be set")
  }

  /**
   * True if source is an existing Frame
   */
  def isFrame: Boolean = {
    sourceType == "frame"
  }

  /**
   * True if source is a pandas Data Frame
   */
  def isClientData: Boolean = {
    sourceType == "strings"
  }

  /**
   * True if source is a file
   */
  def isFieldDelimited: Boolean = {
    sourceType == "file"
  }

  /**
   * True if source is a Line File
   */
  def isFile: Boolean = {
    sourceType == "linefile"
  }

  def isMultilineFile: Boolean = {
    sourceType == "multilinefile" || sourceType == "xmlfile"
  }
}

/**
 *  Describes a Parser
 *
 * @param name Parser name such as  builtin/line/separator
 * @param arguments values necessary for initializing the Parser
 */
case class LineParser(name: String, arguments: LineParserArguments)

/**
 * Values needed for initializing a parser.
 *
 * @param separator Char Separator of a delimated file
 * @param schema Schema of Row created in file
 * @param skip_rows number of lines to skip in the file
 */
case class LineParserArguments(separator: Char, schema: SchemaArgs, skip_rows: Option[Int]) {
  skip_rows match {
    case e: Some[Int] => require(skip_rows.get >= 0, "value for skip_header_lines cannot be negative")
    case _ =>
  }
}

/**
 * Schema arguments for the LineParserArguments -
 * these are arguments supplied by the user rather than our own internal schema representation.
 */
case class SchemaArgs(columns: List[(String, DataType)]) {

  /**
   * Convert args to our internal format
   */
  def schema: Schema = {
    Schema.fromTuples(columns)
  }
}