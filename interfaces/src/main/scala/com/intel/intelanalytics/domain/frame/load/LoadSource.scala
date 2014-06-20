package com.intel.intelanalytics.domain.frame.load

import com.intel.intelanalytics.domain.schema.Schema

/**
 * The case classes in this file are used to parse the json submitted as part of a load or append call
 */

/**
 * Object used for parsing and then executing the frame.append command
 *
 * @param destination DataFrame to load data into. Should be either a uri or id
 * @param source Object describing the data to load into the destination. Includes the Where and How of loading.
 * @tparam frameRef If DataFrame is a uri this should be a String if it is an id it should be a Long
 */
case class Load[frameRef](destination:frameRef, source:LoadSource)

//source_type instead of sourceType so that the parser can properly parse the REST Apis naming convention
/**
 * Describes a resource that should be loaded into a DataFrame
 *
 * @param source_type Source object that can be parsed into an RDD. Such as "file" or "dataframe"
 * @param uri Location of data to load. Should be appropriate for the source_type.
 * @param parser Object describing how to parse the resource. If data already an RDD can be set to None
 */
case class LoadSource(source_type: String, uri: String, parser:Option[LineParser])

/**
 *  Describes a Parser
 *
 * @param name Parser name such as  builtin/line/separator
 * @param arguments values necessary for initializing the Parser
 */
case class LineParser(name: String, arguments:LineParserArguments)

//skip_rows instead of skipRows so that the parser can properly parse the REST Apis naming convention
/**
 * Values needed for initializing a parser.
 *
 * @param separator Char Separator of a delimated file
 * @param schema Schema of Row created in file
 * @param skip_rows number of lines to skip in the file
 */
case class LineParserArguments(separator:Char, schema: Schema, skip_rows:Option[Int])