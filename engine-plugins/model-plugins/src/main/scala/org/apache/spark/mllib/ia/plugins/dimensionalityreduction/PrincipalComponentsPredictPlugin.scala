package org.apache.spark.mllib.ia.plugins.dimensionalityreduction

import com.intel.taproot.analytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.taproot.analytics.domain.model.ModelReference
import com.intel.taproot.analytics.engine.plugin.{ Invocation, ApiMaturityTag }
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

case class PrincipalComponentsPredictArgs(model: ModelReference, frame: FrameReference, observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
}

case class PrincipalComponentsPredictReturn(tSquaredIndex: Double)

class PrincipalComponentsPredictPlugin extends SparkCommandPlugin[PrincipalComponentsPredictArgs, PrincipalComponentsPredictReturn] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:principal_components/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: PrincipalComponentsPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: PrincipalComponentsPredictArgs)(implicit invocation: Invocation): PrincipalComponentsPredictReturn = {
    val models = engine.models
    val frames = engine.frames

    val inputFrame = frames.expectFrame(arguments.frame)
    val modelMeta = models.expectModel(arguments.model)

    //Running MLLib
    val principalComponentJsObject = modelMeta.data.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
    val principalComponentData = principalComponentJsObject.convertTo[PrincipalComponentsData]

    if (arguments.observationColumns.isDefined) {
      require(principalComponentData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }
    val predictColumns = arguments.observationColumns.getOrElse(principalComponentData.observationColumns)

    //create RDD from the frame
    val inputFrameRdd = frames.loadFrameData(sc, inputFrame)

    val rowMatrix: RowMatrix = new RowMatrix(inputFrameRdd.toVectorDenseRDD(predictColumns))
    val eigenVectors = principalComponentData.vFactor
    val y = rowMatrix.multiply(eigenVectors)

    val t = computeTSquareIndex(y.rows, principalComponentData.singularValues, principalComponentData.k)
    PrincipalComponentsPredictReturn(t)
  }

  def computeTSquareIndex(y: RDD[Vector], E: Vector, k: Int): Double = {
    val squaredY = y.map(_.toArray).map(elem => elem.map(x => x * x))
    val vectorOfAccumulators = for (i <- 0 to k - 1) yield y.sparkContext.accumulator(0.0)
    val acc = vectorOfAccumulators.toList
    val t = y.sparkContext.accumulator(0.0)
    squaredY.foreach(row => for (i <- 0 to k - 1) acc(i) += row(i))

    for (i <- 0 to k - 1) {
      if (E(i) > 0) {
        val div = acc(i).value / E(i)
        t += div
      }
    }
    t.value
  }
}