package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ CovarianceMatrixArguments, DataFrame, DataFrameTemplate }
import com.intel.intelanalytics.domain.Naming
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ Column, FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate covariance matrix for the specified columns
 */
class CovarianceMatrixPlugin extends SparkCommandPlugin[CovarianceMatrixArguments, DataFrame] {

  /**
   * The name of the command
   */
  override def name: String = "frame:/covariance_matrix"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate covariance matrix for two or more columns",
    extendedSummary = Some("""
                             |
                             |        .. versionadded:: 0.8
                             | """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CovarianceMatrixArguments) = 3

  /**
   * Calculate covariance matrix for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance matrix
   * @param user current user
   * @return value of type declared as the Return type
   */
  override def execute(invocation: SparkInvocation, arguments: CovarianceMatrixArguments)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // parse arguments
    val frameId: Long = arguments.frame.id
    val frame = frames.expectFrame(frameId)

    val matrixName = if (arguments.matrixName.nonEmpty) {
      Some(arguments.matrixName.get).toString
    }
    else {
      Naming.generateName()
    }

    // load frame as RDD
    val rdd = frames.loadFrameRDD(ctx, frameId).cache()

    //val inputDataColumnNamesAndTypes: List[Column] = arguments.dataColumnNames.map({ row => Column(row, frame.schema.columnTuples(frame.schema.columnIndex(row))._2) }).toList
    val inputDataColumnNamesAndTypes: List[Column] = arguments.dataColumnNames.map({ row => Column(row, DataTypes.float64) }).toList
    val d = arguments.dataColumnNames.length
    val covarianceRDD = Covariance.covarianceMatrix(rdd, arguments.dataColumnNames).cache()

    // create covariance matrix as DataFrame
    val covarianceFrame = frames.create(DataFrameTemplate(matrixName, None))
    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    frames.saveFrame(covarianceFrame, new FrameRDD(schema, rdd))

  }
}