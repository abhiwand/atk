package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrameTemplate, GroupByArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.domain.CreateEntityArgs

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...)
 */
class GroupByPlugin extends SparkCommandPlugin[GroupByArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/group_by"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GroupByArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val ctx = sc

    // validate arguments
    val originalFrame = frames.loadFrameData(ctx, frames.expectFrame(arguments.frame.id))
    val frameSchema = originalFrame.frameSchema
    val aggregationArguments = arguments.aggregations
    val groupByColumns = arguments.groupByColumns.map(columnName => frameSchema.column(columnName))

    // run the operation and save results
    val groupByRdd = GroupByAggregationFunctions.aggregation(originalFrame, groupByColumns, aggregationArguments)

    frames.tryNewFrame(CreateEntityArgs(description = Some("created by group_by command"))) {
      newFrame => frames.saveFrameData(newFrame.toReference, groupByRdd)
    }
  }
}
