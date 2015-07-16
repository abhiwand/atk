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

package com.intel.taproot.analytics.engine.spark.frame.plugins

import com.intel.taproot.analytics.domain.frame.{ UnflattenColumnArgs, FrameEntity }
import com.intel.taproot.analytics.domain.schema.{ Schema, DataTypes, Column }
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.frame.{SparkFrame, RowWrapper}
import com.intel.taproot.analytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.commons.lang.StringUtils
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Take multiple rows and 'unflatten' them into a row with multiple values in a column.
 *
 * Parameters
 * ----------
 * column : List<str>
 * The column list representing the composite key.
 * delimiter : str (optional)
 * The delimiter string.
 * Default is comma (,).
 */
@PluginDoc(oneLine = "Compacts data from multiple rows based on cell data.",
  extended = """Groups together cells in all columns (less the composite key) using "," as string delimiter.
The original rows are deleted. Thr grouping takes place based on a composite key passed as arguments.""")
class UnflattenColumnPlugin extends SparkCommandPlugin[UnflattenColumnArgs, FrameEntity] {

  private val defaultDelimiter = ","

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/unflatten_column"

  override def numberOfJobs(arguments: UnflattenColumnArgs)(implicit invocation: Invocation): Int = 2

  /**
   * Take multiple rows and 'unflatten' them into a row with multiple values in a column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments input specification for column flattening
   * @return a value of type declared as the return type
   */
  override def execute(arguments: UnflattenColumnArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrame = arguments.frame
    val schema = frame.schema
    val compositeKeyNames = arguments.compositeKeyColumnNames
    val compositeKeyIndices = compositeKeyNames.map(schema.columnIndex)

    // run the operation
    val targetSchema = UnflattenColumnFunctions.createTargetSchema(schema, compositeKeyNames)
    val initialRdd = frame.rdd.groupByRows(row => row.values(compositeKeyNames))
    val resultRdd = UnflattenColumnFunctions.unflattenRddByCompositeKey(compositeKeyIndices, initialRdd, targetSchema, arguments.delimiter.getOrElse(defaultDelimiter))

    frame.save(new FrameRdd(targetSchema, resultRdd))
  }

}

object UnflattenColumnFunctions extends Serializable {

  def createTargetSchema(schema: Schema, compositeKeyNames: List[String]): Schema = {
    val keys = schema.copySubset(compositeKeyNames)
    val converted = schema.columnsExcept(compositeKeyNames).map(col => Column(col.name, DataTypes.string))

    keys.addColumns(converted)
  }

  def unflattenRddByCompositeKey(compositeKeyIndex: List[Int],
                                 initialRdd: RDD[(List[Any], Iterable[sql.Row])],
                                 targetSchema: Schema,
                                 delimiter: String): RDD[sql.Row] = {
    val rowWrapper = new RowWrapper(targetSchema)
    val unflattenRdd = initialRdd.map { case (key, row) => key.toArray ++ unflattenRowsForKey(compositeKeyIndex, row, delimiter) }

    unflattenRdd.map(row => rowWrapper.create(row))
  }

  private def unflattenRowsForKey(compositeKeyIndex: List[Int], groupedByRows: Iterable[sql.Row], delimiter: String): Array[Any] = {

    val rows = groupedByRows.toList
    val rowCount = rows.length

    val keySize = compositeKeyIndex.length
    val colsInRow = rows(0).length
    val result = new Array[Any](colsInRow)

    //all but the last line + with delimiter
    for (i <- 0 to rowCount - 2) {
      val row = rows(i)
      addRowToResults(row, compositeKeyIndex, result, delimiter)
    }

    //last line, no delimiter
    val lastRow = rows(rowCount - 1)
    addRowToResults(lastRow, compositeKeyIndex, result, StringUtils.EMPTY)

    result.filter(_ != null)
  }

  private def addRowToResults(row: sql.Row, compositeKeyIndex: List[Int], results: Array[Any], delimiter: String): Unit = {

    for (j <- 0 until row.length) {
      if (!compositeKeyIndex.contains(j)) {
        val value = row.apply(j) + delimiter
        if (results(j) == null) {
          results(j) = value
        }
        else {
          results(j) += value
        }
      }
    }
  }
}
