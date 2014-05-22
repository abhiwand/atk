package com.intel.intelanalytics.engine.spark

import org.scalatest._
import org.apache.spark.SparkContext
import java.util.Date

class SparkJoinSpec extends FlatSpec with Matchers {

  lazy val sc = new SparkContext("local", "test " + new Date())

  "create2TupleForJoin" should "put first column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 0)
    result._1 shouldBe "1"
    result._2 shouldBe Array("2", 3, 4, "5")
  }

  "create2TupleForJoin" should "put third column in first entry" in {
    val data = Array("1", "2", 3, 4, "5")
    val result = SparkOps.create2TupleForJoin(data, 2)
    result._1 shouldBe 3
    result._2 shouldBe Array("1", "2", 4, "5")
  }

  "joinRDDs" should "join two RDD" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](4, 968))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sc.parallelize(id_country_codes).map(t => SparkOps.create2TupleForJoin(t, 0))
    val countryNames = sc.parallelize(id_country_names).map(t => SparkOps.create2TupleForJoin(t, 0))
    val result = SparkOps.joinRDDs(countryCode, countryNames)
    val data = result.take(4)
    val first = data(0)
    first._1 shouldBe 4
    first._2 shouldBe Array(968, "Oman")
  }

}
