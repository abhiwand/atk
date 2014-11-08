package org.apache.spark.mllib.classification.mllib.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.model.{ ModelLoad, Model }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import LogisticRegressionJsonProtocol._
import scala.concurrent.ExecutionContext
import MLLibMethods._

class TestModelPlugin extends SparkCommandPlugin[ModelLoad, Model] {

  override def name: String = "model:logistic_regression/test"

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

      //create RDD from the frame
      val testFrameRDD = frames.loadFrameRDD(ctx, frameId)
      val updatedTestRDD = testFrameRDD.selectColumns(List(arguments.observationColumn, arguments.labelColumn))
      val labeledTestRdd: RDD[LabeledPoint] = createLabeledRdd(updatedTestRDD)

      //Running MLLib
      val logRegJsObject = modelMeta.data.get
      val logRegModel = logRegJsObject.convertTo[LogisticRegressionModel]
      val featureWeights = logRegModel.weights

      //predicting and testing
      val scoreAndLabelRdd: RDD[(Double, Double)] = labeledTestRdd.map { point =>
        val score = logRegModel.predict(point.features)
        (score, point.label)
      }.cache()

      //Run Binary classification metrics -- Using MLLib, can replace with calls to our classification metrics
      outputClassificationMetrics(scoreAndLabelRdd)
      modelMeta
    }

}
