package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ DataFrameTemplate, FrameGroupByColumn, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.{ Await, ExecutionContext }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class GroupByPlugin extends SparkCommandPlugin[FrameGroupByColumn, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the Python layer via code generation.
   */
  override def name: String = "dataframe/group_by"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   *
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: FrameGroupByColumn)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val originalFrameID = arguments.frame.id
    val originalFrame = frames.expectFrame(originalFrameID)
    val schema = originalFrame.schema
    val aggregation_arguments = arguments.aggregations

    // run the operation and save results

    val newFrame = frames.create(DataFrameTemplate(arguments.name, None))
    val args_pair = for {
      (aggregation_function, column_to_apply, new_column_name) <- aggregation_arguments
    } yield (schema.columns.indexWhere(columnTuple => columnTuple._1 == column_to_apply), aggregation_function)

    if (arguments.groupByColumns.length > 0) {
      val groupByColumns = arguments.groupByColumns

      val columnIndices: Seq[(Int, DataType)] = for {
        col <- groupByColumns
        columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
        columnDataType = schema.columns(columnIndex)._2
      } yield (columnIndex, columnDataType)

      val groupedRDD = frames.loadFrameRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => {
        for { index <- columnIndices.map(_._1) } yield data(index)
      }.mkString("\0"))
      val resultRdd = GroupByAggregationFunctions.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, columnIndices.map(_._2).toArray, arguments)
      frames.saveFrame(newFrame, resultRdd)
    }
    else {
      val groupedRDD = frames.loadFrameRdd(ctx, originalFrameID).groupBy((data: Rows.Row) => "")
      val resultRdd = GroupByAggregationFunctions.aggregation(groupedRDD, args_pair, originalFrame.schema.columns, Array[DataType](), arguments)
      frames.saveFrame(newFrame, resultRdd)
    }
  }
}
