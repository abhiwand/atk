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

import com.intel.intelanalytics.domain.frame.{ UnflattenColumnArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes, Column }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ RowWrapper }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.commons.lang.StringUtils
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Take multiple rows and 'unflatten' them into a row with multiple values in a column.
 */
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

    val frames = engine.frames
    val frameEntity = frames.expectFrame(arguments.frame)
    val schema = frameEntity.schema
    val compositeKeyNames = arguments.compositeKeyColumnNames
    val compositeKeyIndices = compositeKeyNames.map(frameEntity.schema.columnIndex)

    // run the operation
    val targetSchema = UnflattenColumnFunctions.createTargetSchema(schema, compositeKeyNames)
    val initialRdd = frames.loadFrameData(sc, frameEntity).groupByRows(row => row.values(compositeKeyNames))
    val resultRdd = UnflattenColumnFunctions.unflattenRddByCompositeKey(compositeKeyIndices, initialRdd, targetSchema, arguments.delimiter.getOrElse(defaultDelimiter))

    frames.saveFrameData(frameEntity.toReference, new FrameRdd(targetSchema, resultRdd))
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
