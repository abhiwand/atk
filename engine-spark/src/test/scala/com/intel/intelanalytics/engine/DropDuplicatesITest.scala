package com.intel.intelanalytics.engine.spark

import org.scalatest.{ BeforeAndAfterEach, Matchers, FlatSpec }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.intel.testutils.TestingSparkContextFlatSpec

class DropDuplicatesITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "removeDuplicatesByKey" should "keep only 1 rows per key" in {

    //setup test data
    val favoriteMovies = List(Array[Any]("John", 1, "Titanic"), Array[Any]("Kathy", 2, "Jurassic Park"), Array[Any]("John", 1, "The kite runner"), Array[Any]("Kathy", 2, "Toy Story 3"), Array[Any]("Peter", 3, "Star War"))
    val rdd = sc.parallelize(favoriteMovies)

    rdd.count() shouldBe 5

    //prepare a pair rdd for removing duplicates
    val pairRdd = rdd.map(row => SparkOps.createKeyValuePairFromRow(row, Seq(0, 1)))

    //remove duplicates identified by key
    val duplicatesRemoved = SparkOps.removeDuplicatesByKey(pairRdd)
    duplicatesRemoved.count() shouldBe 3 // original data contain 5 rows, now drop to 3

    //transform output to a sortable format
    val sortable = duplicatesRemoved.map(t => SparkOps.createKeyValuePairFromRow(t, Seq(1))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]

    //sort output to validate result
    val sorted = sortable.sortByKey(true)

    //matching the result
    val data = sorted.take(4)
    data(0)._2 shouldBe Array[Any]("John", 1, "Titanic")
    data(1)._2 shouldBe Array[Any]("Kathy", 2, "Jurassic Park")
    data(2)._2 shouldBe Array[Any]("Peter", 3, "Star War")
  }
}
