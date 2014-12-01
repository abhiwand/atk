package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.multivariatestatistics

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ CovarianceArguments, CovarianceReturn }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance.Covariance
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate covariance for the specified columns
 */
class CovariancePlugin extends SparkCommandPlugin[CovarianceArguments, CovarianceReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame:/covariance"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate covariance for exactly two columns",
    extendedSummary = Some("""
                             |
                             |        .. versionadded:: 0.8
                             | """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CovarianceArguments) = 3

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @param user current user
   * @return value of type declared as the Return type
   */
  override def execute(invocation: SparkInvocation, arguments: CovarianceArguments)(implicit user: UserPrincipal, executionContext: ExecutionContext): CovarianceReturn = {
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext
    // parse arguments
    val frameId: Long = arguments.frame.id
    // load frame as RDD
    val rdd = frames.loadFrameRDD(ctx, frameId).cache()
    Covariance.covariance(rdd, arguments.dataColumnNames)
  }

}