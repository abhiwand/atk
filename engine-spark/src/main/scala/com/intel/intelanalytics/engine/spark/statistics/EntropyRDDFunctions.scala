package com.intel.intelanalytics.engine.spark.statistics

import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.rdd.RDD

import scala.math.log
import scala.util.Try

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

/**
 * Functions for computing entropy.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private[spark] object EntropyRDDFunctions extends Serializable {

  /**
   * Calculate the Shannon entropy for specified column in data frame.
   *
   * @param frameRDD RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @return Weighted shannon entropy (using natural log)
   */
  def shannonEntropy(frameRDD: RDD[Row],
                     dataColumnIndex: Int,
                     weightsColumnIndexOption: Option[Int] = None,
                     weightsTypeOption: Option[DataType] = None): Double = {
    require(dataColumnIndex >= 0, "column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, frameRDD)
        .filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val distinctCountRDD = dataWeightPairs.reduceByKey(_ + _).map({ case (value, count) => count })

    // sum() throws an exception if RDD is empty so catching it and returning zero
    val totalCount = Try(distinctCountRDD.sum()).getOrElse(0d)

    val entropy = if (totalCount > 0) {
      val distinctProbabilities = distinctCountRDD.map(count => count / totalCount)
      -distinctProbabilities.map(probability => if (probability > 0) probability * log(probability) else 0).sum()
    }
    else 0d

    entropy
  }
}
