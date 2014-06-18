package com.intel.intelanalytics.domain.frame.load

import com.intel.intelanalytics.domain.schema.Schema

case class Load[frameRef](destination:frameRef, source:LoadSource)

//source_type instead of sourceType so that the parser can properly parse the REST Apis naming convention
case class LoadSource(source_type: String, uri: String, parser:Option[LineParser])

case class LineParser(name: String, arguments:LineParserArguments)

case class LineParserArguments(separator:Char, schema: Schema, skip_rows:Option[Int])