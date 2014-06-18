package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.frame.LoadLines
import com.intel.intelanalytics.domain.frame.load.{Load, LineParserArguments, LineParser, LoadSource}
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import spray.json._
import DomainJsonProtocol._

/**
 * Created by rhicke on 6/12/14.
 */
class LoadLinesTest extends FlatSpec with Matchers{


  it should "be parsable from a JSON object" in {
       val string =
         """
           |{
           |    "source": "temp/simple.csv",
           |    "destination": "http://localhost:9099/v1/dataframes/5",
           |    "skipRows": 0,
           |    "lineParser": {
           |      "operation": {
           |        "name": "builtin/line/separator"
           |      },
           |      "arguments": {
           |        "separator": ","
           |      }
           |    },
           |    "schema": {
           |      "columns": [["a", "int32"], ["b", "int32"]]
           |    }
           |  }
         """.stripMargin
      val myJson = JsonParser(string).asJsObject
      val myLoadLines = myJson.convertTo[LoadLines[JsObject, String]]
      myLoadLines.source should be("temp/simple.csv")
      println(myLoadLines.lineParser)
  }

  "Load" should "parse a Load object with a LinesSource" in {
    val string =
      """
        |{
        |    "destination": "http://localhost:9099/v1/dataframes/5",
        |    "source": {
        |      "source_type": "file",
        |      "uri": "m1demo/domains.json",
        |      "parser": {
        |        "name": "builtin/line/separator",
        |        "arguments": {
        |          "separator": "`",
        |          "skip_rows": 0,
        |          "schema": {
        |            "columns": [
        |              ["json", "str"]
        |            ]
        |          }
        |        }
        |      }
        |    }
        |}
        |
      """.stripMargin
//    println(string)
    val myJson = JsonParser(string).asJsObject
//    println(myJson)
    val myLoadLines = myJson.convertTo[Load[String]]
    println(myLoadLines)
    myLoadLines.source.uri should be("m1demo/domains.json")
  }

  "LoadSource" should "be parsed from a JSON that does include a parser" in{
    val json =
      """
        |{
        |  "source_type": "file",
        |  "uri": "m1demo/domains.json",
        |  "parser": {
        |    "name": "builtin/line/separator",
        |    "arguments": {
        |      "separator": "`",
        |      "skip_rows": 0,
        |      "schema": {
        |        "columns": [
        |          ["json", "str"]
        |        ]
        |      }
        |    }
        |  }
        |}
      """.stripMargin
    val myJson = JsonParser(json).asJsObject
    val mySource = myJson.convertTo[LoadSource]
    mySource.source_type should be("file")
    mySource.uri  should be("m1demo/domains.json")
    println(mySource)
    mySource.parser should not be(None)
  }

  "LoadSource" should "be parsed from a JSON that does not include a parser" in{
    val json =
      """
        |{
        |  "source_type": "dataframe",
        |  "uri": "temp/simple.csv"
        |}
      """.stripMargin
    val myJson = JsonParser(json).asJsObject
    val mySource = myJson.convertTo[LoadSource]
    mySource.source_type should be("dataframe")
    mySource.uri  should be("temp/simple.csv")
    println(mySource)
    mySource.parser should be(None)
  }


  "Parser" should "be parsed from a JSON" in{
    val json =
      """{
        |        "name": "builtin/line/separator",
        |        "arguments": {
        |            "separator": "`",
        |            "skip_rows": 0,
        |            "schema": {
        |                "columns": [
        |                    ["json","str"]
        |                ]
        |            }
        |        }
        |    }
      """.stripMargin
    val myJson = JsonParser(json).asJsObject
    val mySource = myJson.convertTo[LineParser]
//    mySource.sourceType should be("dataframe")
//    mySource.uri  should be("temp/simple.csv")
    println(mySource)
  }

  "ParserArguments" should "be parsed from a JSON" in{
    val json =
      """{
        |            "separator": "`",
        |            "skip_rows": 0,
        |            "schema": {
        |                "columns": [
        |                    ["json","str"]
        |                ]
        |            }
        |        }
      """.stripMargin
    val myJson = JsonParser(json).asJsObject
    val mySource = myJson.convertTo[LineParserArguments]
    //    mySource.sourceType should be("dataframe")
    //    mySource.uri  should be("temp/simple.csv")
    println(mySource)
  }



}
