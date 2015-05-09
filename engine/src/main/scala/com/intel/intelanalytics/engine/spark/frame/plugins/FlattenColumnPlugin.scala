//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FlattenColumnArgs }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import java.util.regex.Pattern

/**
 * Take a row with multiple values in a column and 'flatten' it into multiple rows.
 */
class FlattenColumnPlugin extends SparkCommandPlugin[FlattenColumnArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/flatten_column"

  override def numberOfJobs(arguments: FlattenColumnArgs)(implicit invocation: Invocation): Int = 2

  /**
   * Take a row with multiple values in a column and 'flatten' it into multiple rows.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments input specification for column flattening
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FlattenColumnArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameEntity = frames.expectFrame(arguments.frame)
    var schema = frameEntity.schema
    var flattener: RDD[Row] => RDD[Row] = null
    val columnIndex = frameEntity.schema.columnIndex(arguments.column)
    val columnDataType = frameEntity.schema.columnDataType(arguments.column)
    columnDataType match {
      case DataTypes.string => flattener = FlattenColumnFunctions.flattenRddByStringColumnIndex(columnIndex, arguments.delimiter.getOrElse(","))
      case DataTypes.vector(length) =>
        schema = schema.convertType(arguments.column, DataTypes.float64)
        flattener = FlattenColumnFunctions.flattenRddByVectorColumnIndex(columnIndex, length)
      case _ => throw new IllegalArgumentException(s"Flatten column does not support type $columnDataType")
    }

    // run the operation
    val rdd = frames.loadLegacyFrameRdd(sc, frameEntity)
    val flattenedRDD = flattener(rdd)

    // save results
    frames.saveLegacyFrame(frameEntity.toReference, new LegacyFrameRdd(schema, flattenedRDD))
  }

}

/**
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object FlattenColumnFunctions extends Serializable {

  /**
   * Flatten RDD by the column with specified column index
   * @param index column index
   * @param rdd RDD for flattening
   * @return new RDD with column flattened
   */
  def flattenRddByVectorColumnIndex(index: Int, vectorLength: Long)(rdd: RDD[Row]): RDD[Row] = {
    val flattener = flattenRowByVectorColumnIndex(index, vectorLength)_
    rdd.flatMap(row => flattener(row))
  }

  /**
   * Flatten RDD by the column with specified column index
   * @param index column index
   * @param separator separator for splitting
   * @param rdd RDD for flattening
   * @return new RDD with column flattened
   */
  def flattenRddByStringColumnIndex(index: Int, separator: String)(rdd: RDD[Row]): RDD[Row] = {
    val flattener = flattenRowByStringColumnIndex(index, separator)_
    rdd.flatMap(row => flattener(row))
  }

  /**
   * flatten a row by the column with specified column index.  Column must be a vector
   * @param index column index
   * @param row row data
   * @return flattened out row/rows
   */
  private[frame] def flattenRowByVectorColumnIndex(index: Int, vectorLength: Long)(row: Array[Any]): Array[Array[Any]] = {
    DataTypes.toVector(vectorLength)(row(index)).toArray.map(s => {
      val r = row.clone()
      r(index) = s
      r
    })
  }

  /**
   * flatten a row by the column with specified column index.  Column must be a string
   * Eg. for row (1, "dog,cat"), flatten by second column will yield (1,"dog") and (1,"cat")
   * @param index column index
   * @param row row data
   * @param delimiter separator for splitting
   * @return flattened out row/rows
   */
  private[frame] def flattenRowByStringColumnIndex(index: Int, delimiter: String)(row: Array[Any]): Array[Array[Any]] = {
    val splitted = row(index).asInstanceOf[String].split(Pattern.quote(delimiter))
    splitted.map(s => {
      val r = row.clone()
      r(index) = s
      r
    })
  }
}