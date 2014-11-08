package org.apache.spark.mllib.classification.mllib.plugins

import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.model.{ ModelLoad, Model }
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import scala.concurrent.ExecutionContext

//Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import LogisticRegressionJsonProtocol._
import MLLibMethods._

class TrainModelPlugin extends SparkCommandPlugin[ModelLoad, Model] {

  override def name: String = "model:logistic_regression/train"

  override def doc: Option[CommandDoc] = None

  override def numberOfJobs(arguments: ModelLoad) = 2

  override def execute(invocation: SparkInvocation, arguments: ModelLoad)(implicit user: UserPrincipal, executionContext: ExecutionContext): Model =
    {
      val models = invocation.engine.models
      val frames = invocation.engine.frames
      val fsRoot = invocation.engine.fsRoot
      val ctx = invocation.sparkContext

      //validate arguments
      val frameId = arguments.frame.id
      val modelId = arguments.model.id

      val inputFrame = frames.expectFrame(frameId)
      val modelMeta = models.expectModel(modelId)
      //TODO: Need to initialize the type

      //require(modelMeta.isLogisticRegression(), "wrong type ...")

      //create RDD from the frame
      val trainFrameRDD = frames.loadFrameRDD(ctx, frameId)
      val updatedTrainRDD = trainFrameRDD.selectColumns(List(arguments.observationColumn, arguments.labelColumn))
      val labeledTrainRdd: RDD[LabeledPoint] = createLabeledRdd(updatedTrainRDD)

      //Running MLLib
      val logReg = new LogisticRegressionWithSGD()
      val logRegModel = logReg.run(labeledTrainRdd)
      val modelObject = logRegModel.toJson.asJsObject

      models.updateModel(modelMeta, modelObject)
      models.loadModel(arguments, invocation)(user)
    }

}
