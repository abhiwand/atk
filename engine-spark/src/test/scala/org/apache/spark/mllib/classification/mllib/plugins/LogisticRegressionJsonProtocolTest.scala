package org.apache.spark.mllib.classification.mllib.plugins

import org.apache.spark.mllib.linalg.{SparseVector, DenseVector}
import org.scalatest.WordSpec
import spray.json._
import LogisticRegressionJsonProtocol._

class LogisticRegressionJsonProtocolTest extends WordSpec {
  "DenseVectorFormat" should {

    "be able to serialize" in {
      val dv = new DenseVector(Array(1.2,3.4,2.2))
      assert(dv.toJson.compactPrint == "{\"values\":[1.2,3.4,2.2]}")
    }

    "parse json" in {
      val string =
        """
          |{
          |   "values": [1.2,3.4,5.6,7.8]
          |
          |
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val dv = json.convertTo[DenseVector]
      assert(dv.values.length == 4)
    }
  }

  "SparseVectorFormat" should {

    "be able to serialize" in{
      val sv = new SparseVector(2, Array(1,2,3),Array(1.5,2.5,3.5))
      assert(sv.toJson.compactPrint == "{\"size\":2,\"indices\":[1,2,3],\"values\":[1.5,2.5,3.5]}")
    }

    "parse json" in {
      val string =
      """
        |{
        |   "size": 3,
        |   "indices": [1,2,3,4],
        |   "values": [1.5,2.5,3.5,4.5]
        |
        |
        | }
      """.stripMargin
      val json = JsonParser(string).asJsObject
      val sv = json.convertTo[SparseVector]
      assert(sv.size == 3)
      assert(sv.indices.length == 4)
      assert(sv.values.length ==4)
    }
  }


}
