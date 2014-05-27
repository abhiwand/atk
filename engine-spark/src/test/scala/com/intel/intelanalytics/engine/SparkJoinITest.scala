package com.intel.intelanalytics.engine.spark
import org.scalatest.{ BeforeAndAfterEach, Matchers, FlatSpec }
import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class SparkJoinITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContext {

  "joinRDDs" should "join two RDD with inner join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](4, 968))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sc.parallelize(id_country_codes).map(t => SparkOps.create2TupleForJoin(t, 0))
    val countryNames = sc.parallelize(id_country_names).map(t => SparkOps.create2TupleForJoin(t, 0))

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "inner")
    val sortable = result.map(t => SparkOps.create2TupleForJoin(t, 0)).asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)
    data(0)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(1)._2 shouldBe Array(2, 91, 2, "India")
    data(2)._2 shouldBe Array(3, 47, 3, "Norway")
    data(3)._2 shouldBe Array(4, 968, 4, "Oman")
  }

  "joinRDDs" should "join two RDD with left join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](4, 968))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"))

    val countryCode = sc.parallelize(id_country_codes).map(t => SparkOps.create2TupleForJoin(t, 0))
    val countryNames = sc.parallelize(id_country_names).map(t => SparkOps.create2TupleForJoin(t, 0))

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "left")
    val sortable = result.map(t => SparkOps.create2TupleForJoin(t, 0)).asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)
    data(0)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(1)._2 shouldBe Array(2, 91, 2, "India")
    data(2)._2 shouldBe Array(3, 47, 3, "Norway")
    data(3)._2 shouldBe Array(4, 968, null, null)
  }

  "joinRDDs" should "join two RDD with right join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sc.parallelize(id_country_codes).map(t => SparkOps.create2TupleForJoin(t, 0))
    val countryNames = sc.parallelize(id_country_names).map(t => SparkOps.create2TupleForJoin(t, 0))

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "right")
    val sortable = result.map(t => SparkOps.create2TupleForJoin(t, 2)).asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)

    data(0)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(1)._2 shouldBe Array(2, 91, 2, "India")
    data(2)._2 shouldBe Array(3, 47, 3, "Norway")
    data(3)._2 shouldBe Array(null, null, 4, "Oman")
  }

}
