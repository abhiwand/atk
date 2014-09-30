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

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.frame.FrameGroupByColumn
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.rdd.RDD
import spray.json.JsObject

/**
 * Aggregations for Frames (SUM, COUNT, etc)
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object GroupByAggregationFunctions {

  def aggregation(groupedRDD: RDD[(String, Iterable[Array[Any]])],
                  args_pair: Seq[(Int, String)],
                  schema: List[(String, DataTypes.DataType)],
                  groupedColumnSchema: Array[DataTypes.DataType],
                  arguments: FrameGroupByColumn): FrameRDD = {

    val resultRdd = groupedRDD.map(elem =>
      convertGroupBasedOnSchema(groupedColumnSchema, elem._1) ++ aggregationFunctions(elem._2.toSeq, args_pair, schema))

    val aggregated_column_schema = for {
      i <- args_pair
    } yield {
      i._2 match {
        case "COUNT" | "COUNT_DISTINCT" => DataTypes.int32
        case "SUM" | "MIN" | "MAX" => schema(i._1)._2
        case _ => DataTypes.float64
      }
    }

    val new_data_types = groupedColumnSchema ++ aggregated_column_schema

    val new_column_names = arguments.groupByColumns ++ {
      for { i <- arguments.aggregations } yield i._3
    }
    val new_schema = new_column_names.zip(new_data_types)

    new FrameRDD(new Schema(new_schema), resultRdd)
  }

  private def convertGroupBasedOnSchema(groupedColumnSchema: Array[DataTypes.DataType], data: String): Array[Any] = {
    if (data != null && data != "")
      DataTypes.parseMany(groupedColumnSchema)(data.split("\0"))
    else
      Array[Any]()
  }

  private def aggregationFunctions(elem: Seq[Array[Any]],
                                   args_pair: Seq[(Int, String)],
                                   schema: List[(String, DataTypes.DataType)]): Seq[Any] = {
    for {
      i <- args_pair
    } yield (i, schema(i._1)._2) match {
      case ((j: Int, "SUM"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).sum
      case ((j: Int, "SUM"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).sum
      case ((j: Int, "SUM"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).sum
      case ((j: Int, "SUM"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).sum
      case ((j: Int, "MAX"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).max
      case ((j: Int, "MAX"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).max
      case ((j: Int, "MAX"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).max
      case ((j: Int, "MAX"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).max
      case ((j: Int, "MIN"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).min
      case ((j: Int, "MIN"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).min
      case ((j: Int, "MIN"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).min
      case ((j: Int, "MIN"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).min
      case ((j: Int, "AVG"), DataTypes.int32) => elem.map(e => e(j).asInstanceOf[Int]).sum * 1.0 / elem.length
      case ((j: Int, "AVG"), DataTypes.int64) => elem.map(e => e(j).asInstanceOf[Long]).sum * 1.0 / elem.length
      case ((j: Int, "AVG"), DataTypes.float32) => elem.map(e => e(j).asInstanceOf[Float]).sum / elem.length
      case ((j: Int, "AVG"), DataTypes.float64) => elem.map(e => e(j).asInstanceOf[Double]).sum / elem.length

      case ((j: Int, "COUNT"), _) => elem.length
      case ((j: Int, "COUNT_DISTINCT"), _) => elem.map(e => e(j)).distinct.length

      case ((j: Int, "VAR"), DataTypes.int32) =>
        val elements: Seq[Int] = elem.map(e => e(j).asInstanceOf[Int])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length
      case ((j: Int, "VAR"), DataTypes.int64) =>
        val elements: Seq[Long] = elem.map(e => e(j).asInstanceOf[Long])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length
      case ((j: Int, "VAR"), DataTypes.float32) =>
        val elements: Seq[Float] = elem.map(e => e(j).asInstanceOf[Float])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length
      case ((j: Int, "VAR"), DataTypes.float64) =>
        val elements: Seq[Double] = elem.map(e => e(j).asInstanceOf[Double])
        val mean = elements.sum / elements.length
        elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length

      case ((j: Int, "STDEV"), DataTypes.int32) =>
        val elements: Seq[Int] = elem.map(e => e(j).asInstanceOf[Int])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)
      case ((j: Int, "STDEV"), DataTypes.int64) =>
        val elements: Seq[Long] = elem.map(e => e(j).asInstanceOf[Long])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)
      case ((j: Int, "STDEV"), DataTypes.float32) =>
        val elements: Seq[Float] = elem.map(e => e(j).asInstanceOf[Float])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)
      case ((j: Int, "STDEV"), DataTypes.float64) =>
        val elements: Seq[Double] = elem.map(e => e(j).asInstanceOf[Double])
        val mean = elements.sum / elements.length
        Math.sqrt(elements.foldLeft(0.0)((r, c) => r + Math.pow(c - mean, 2)) / elements.length)

      case _ => throw new IllegalArgumentException(s"Invalid aggregation function $i._2")
    }
  }
}
