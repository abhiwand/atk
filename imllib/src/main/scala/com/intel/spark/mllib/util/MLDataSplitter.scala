//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package com.intel.spark.mllib.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.reflect.ClassTag
import org.apache.spark.SparkException
import scala.util.Random

/**
 * Class that represents the entries (content strings) and labels of a data point.
 *
 * @param label for this data point.
 * @param entry (content string) for this data point.
 */
case class LabeledLine[T: ClassTag](label: Int, entry: T)

/**
 * Data Splitter for ML algorithms. It randomly labels an input RDD with user 
 * specified percentage for each category.
 *
 * @param percentages: A double array stores percentages.
 * @param seed: Random seed for random number generator.
 */
class MLDataSplitter(percentages: Array[Double], seed: Int) extends Serializable {
  
  // verify percentages
  if (!percentages.forall(p => p > 0)) {
    throw new SparkException("Some percentages are negative numbers.")
  }
  val sum = percentages.reduceLeft(_ + _)
  if (sum != 1.0D) {
    throw new SparkException("Summation of percentages isn't equal to 1.")
  }    

  var cdf = percentages.scanLeft(0D)(_ + _)
  cdf = cdf.drop(1)

  /**
   * Randomly label each entry of an input RDD according to user specified percentage
   * for each category.
   *
   * @param inputRDD RDD of type T.
   */
  def randomlyLabelRDD[T: ClassTag](inputRDD: RDD[T]): RDD[LabeledLine[T]] = {
    // generate auxiliary (sample) RDD
    val auxiliaryRDD = new AuxiliaryRDD(inputRDD, seed).cache()
    val labeledRDD = inputRDD.zip(auxiliaryRDD).map { p =>
          val line = p._1
          val sampleValue = p._2
          var label = 0
          while (sampleValue > cdf(label)) {
            label += 1
          }
          LabeledLine(label, line)
    }
    labeledRDD
  }

}

/**
 * Top-level methods for calling MLDataSplitter.
 */
object MLDataSplitter {

  def main(args: Array[String]) {
    if (args.length != 7) {
      println("Usage: MLDataSplitter <master> <input_dir> <output_dir> <percentages> <random_seed> <memory_size> <jar_file>")
      System.exit(1)
    }
    val master = args(0)
    val input = args(1)
    val output = args(2)
    val percentages = args(3).split(',').map(_.split(':'))
    val seed = args(4).toInt
    val memorySize = args(5)
    val jarFile = args(6)

    // verify percentages
    val partitionNames = percentages.map(_(0))
    val partitionPercentages = percentages.map(_(1).toDouble)
    val splitter = new MLDataSplitter(partitionPercentages, seed)

    // set up spark context
    val conf = new SparkConf()
                    .setMaster(master)
                    .setAppName("MLDataSplitter")
                    .set("spark.executor.memory", memorySize)
                    .setJars(Seq(jarFile))
    val sc = new SparkContext(conf)

    // load data for sampling/splitting
    val inputRDD  = sc.textFile(input).cache()
    println("Number of lines in input files: %d".format(inputRDD.count()))
    
    // split RDD randomly
    val labeledRDD = splitter.randomlyLabelRDD(inputRDD)

    (0 until percentages.size).foreach { i =>
      val partitionName = partitionNames(i)
      val partitionRDD = labeledRDD.filter(p => p.label == i).map(_.entry)
      partitionRDD.saveAsTextFile(output + "/" + partitionName)
      println("Number of lines in partition %s: %d".format(partitionName, partitionRDD.count))
    }

    sc.stop()
  }
}
