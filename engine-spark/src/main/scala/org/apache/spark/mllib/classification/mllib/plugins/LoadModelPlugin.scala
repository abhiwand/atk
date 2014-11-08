package org.apache.spark.mllib.classification.mllib.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.model.{ ModelLoad, Model }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.{ LinearRegressionWithSGD, LinearRegressionModel }

import scala.concurrent.ExecutionContext

//Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class LoadModelPlugin extends SparkCommandPlugin[ModelLoad, Model] {

  override def name: String = "model/load"

  override def doc: Option[CommandDoc] = None

  override def numberOfJobs(arguments: ModelLoad) = 2

  override def execute(invocation: SparkInvocation, arguments: ModelLoad)(implicit user: UserPrincipal, executionContext: ExecutionContext): Model =
    {
      //println("\n in model/load")
      val models = invocation.engine.models
      val frames = invocation.engine.frames
      val fsRoot = invocation.engine.fsRoot
      val ctx = invocation.sparkContext

      //validate arguments
      val frameId = arguments.frame.id

      val inputFrame = frames.expectFrame(frameId)

      //run the operation
      val trainFrameRDD = frames.loadFrameRDD(ctx, frameId)
      models.loadModel(arguments, invocation)(user)
    }
}
