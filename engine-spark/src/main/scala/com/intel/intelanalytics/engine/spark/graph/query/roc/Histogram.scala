package com.intel.intelanalytics.engine.spark.graph.query.roc

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Histogram {

  val epsilon = 0.000001

  /**
   * Create evenly-spaced buckets for histogram.
   *
   * Spark's histogram(Int) method is throwing ArrayOutOfBounds exceptions so this is a work-around.
   *
   * @param numBuckets Number
   * @param min Minimum value for histogram
   * @param max Maximum value for histogram
   * @return Histogram buckets
   */
  def makeBuckets(numBuckets: Int, min: Double, max: Double): Array[Double] = {
    require(numBuckets > 0, "Number of buckets for histogram should be greater than zero.")
    require(max > min, "Maximum value should exceed minimum value for histogram buckets.")
    require(!min.isNaN && !max.isNaN && !min.isInfinite && !max.isInfinite, "Values should not contain +/-infinity or NaN")

    val bucketWidth = (max - min) / numBuckets
    (0 to numBuckets).map(x => min + bucketWidth * x).toArray
  }

  /**
   * Create histogram.
   */
  def getHistogram(rdd: RDD[Double], numBuckets: Int): Histogram = {
    val (min, max) = getMinMax(rdd)
    val buckets = makeBuckets(numBuckets, min, max)
    val counts = rdd.histogram(buckets)
    Histogram(buckets, counts)
  }

  /**
   * Get the minimum and maximum values for RDD. Method is based on Spark's DoubleRDDFunctions.
   *
   * @param rdd RDD of doubles
   * @return Tuple with minimum and maximum values for RDD
   */
  def getMinMax(rdd: RDD[Double]): (Double, Double) = {
    val (max: Double, min: Double) = rdd.mapPartitions { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
          (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    if (max == Double.NegativeInfinity && min == Double.PositiveInfinity) { //Empty RDD
      (Double.NegativeInfinity, Double.PositiveInfinity)
    }
    else {
      (min, max)
    }
  }
}

/**
 * Histogram comprising of buckets and corresponding counts.
 *
 * @param buckets Array of buckets. The buckets are all open to the left except for the last which is closed
 *                e.g. for the array [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
 *
 * @param counts Array of corresponding counts. The size of this array is smaller than the buckets array by 1.
 */
case class Histogram(buckets: Array[Double], counts: Array[Long])