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

package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.frame.LoadLines
import com.intel.intelanalytics.domain.frame.load.{ Load, LineParserArguments, LineParser, LoadSource }
import com.intel.intelanalytics.domain.query.{ QueryResultFactory, QueryResult }
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import spray.json._
import DomainJsonProtocol._

class LoadLinesTest extends FlatSpec with Matchers {
  "Load" should "parse a Load object with for a file source" in {
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
    val myJson = JsonParser(string).asJsObject
    val myLoadLines = myJson.convertTo[Load[String]]

    myLoadLines.source.uri should be("m1demo/domains.json")
    myLoadLines.source.source_type should be("file")
    myLoadLines.source.parser should not be (None)
    val parser = myLoadLines.source.parser.get

    parser.name should be("builtin/line/separator")
    parser.arguments should be(LineParserArguments('`', Schema(List(("json", DataTypes.string))), Some(0)))
  }

  "Load" should "parse a Load object with for a dataframe source" in {
    val string =
      """
        |{
        |    "destination": "http://localhost:9099/v1/dataframes/5",
        |    "source": {
        |      "source_type": "dataframe",
        |      "uri": "http://localhost:9099/v1/dataframes/5"
        |    }
        |}
        |
      """.stripMargin
    val myJson = JsonParser(string).asJsObject
    val myLoadLines = myJson.convertTo[Load[String]]

    myLoadLines.source.uri should be("http://localhost:9099/v1/dataframes/5")
    myLoadLines.source.source_type should be("dataframe")
    myLoadLines.source.parser should be(None)
  }

  "LoadSource" should "be parsed from a JSON that does include a parser" in {
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
    mySource.uri should be("m1demo/domains.json")

    mySource.parser should not be (None)
  }

  "QueryResult" should "become a JsObject" in {
    val l: Iterable[Array[Any]] = Iterable(Array(null, 50, "hello"), Array(5.0), Array(1), Array(2), Array(3))

    val q = QueryResult(l.map(row => row.map {
      case null => JsNull
      case a => a.toJson
    }.toJson).toList)
    println(q)
    println(q.toJson)
    println(q.toJson.asJsObject())
    val q2 = QueryResultFactory.buildFromRows(l)
    println(q2.toJson)
  }

}
