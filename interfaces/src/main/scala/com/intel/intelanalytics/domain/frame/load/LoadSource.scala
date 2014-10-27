package com.intel.intelanalytics.domain.frame.load

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.Schema

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

//source_type instead of sourceType so that the parser can properly parse the REST Apis naming convention
/**
 * Describes a resource that should be loaded into a DataFrame
 *
 * @param source_type Source object that can be parsed into an RDD. Such as "file" or "frame"
 * @param uri Location of data to load. Should be appropriate for the source_type.
 * @param parser Object describing how to parse the resource. If data already an RDD can be set to None
 */
case class LoadSource(source_type: String, uri: String, parser: Option[LineParser]) {

  require(source_type != null)
  require(source_type == "frame" || source_type == "file")
  require(uri != null)
  require(parser != null)

  /**
   * True if source is an existing Frame
   */
  def isFrame: Boolean = {
    source_type == "frame"
  }

  /**
   * True if source is a file
   */
  def isFile: Boolean = {
    source_type == "file"
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
case class LineParserArguments(separator: Char, schema: SchemaArgs, skip_rows: Option[Int])

/**
 * Schema arguments for the LineParserArguments -
 * these are arguments supplied by the user rather than our own internal schema representation.
 */
case class SchemaArgs(columns: List[(String, DataType)]) {

  /**
   * Convert args to our internal format
   */
  def schema: Schema = {
    new Schema(columns)
  }
}