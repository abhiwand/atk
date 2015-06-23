/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.frame.{ FrameEntity, FlattenColumnArgs }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.schema.{ DataTypes }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import java.util.regex.Pattern

/**
 * Take a row with multiple values in a column and 'flatten' it into multiple rows.
 *
 * Parameters
 * ----------
 * column : str
 *   The column to be flattened.
 * delimiter : str (optional)
 *   The delimiter string.
 *   Default is comma (,).
 */
@PluginDoc(oneLine = "Spread data to multiple rows based on cell data.",
  extended = """Splits cells in the specified column into multiple rows according to a string
delimiter.
New rows are a full copy of the original row, but the specified column only
contains one value.
The original row is deleted.""")
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
