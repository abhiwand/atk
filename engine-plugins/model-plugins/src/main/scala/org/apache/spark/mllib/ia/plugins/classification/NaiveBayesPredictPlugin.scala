package org.apache.spark.mllib.ia.plugins.classification

import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.taproot.analytics.domain.model.ModelReference
import com.intel.taproot.analytics.domain.schema.DataTypes
<<<<<<< HEAD
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, PluginDoc, ApiMaturityTag, Invocation }
import com.intel.taproot.analytics.engine.spark.frame.SparkFrameData
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
=======
import com.intel.taproot.analytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
>>>>>>> master
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vectors
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

case class NaiveBayesPredictArgs(@ArgDoc(""" """) model: ModelReference,
                                 @ArgDoc("""A frame whose labels are
to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference,
                                 @ArgDoc("""Column(s) containing the
observations whose labels are to be predicted.
By default, we predict the labels over columns the NaiveBayesModel
was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")

}
/*
Make new frame with column for label prediction.

Predict the labels for a test frame and create a new frame revision with
existing columns and a new predicted label's column.

Returns
-------
Frame
    Frame containing the original frame's columns and a column with the
    predicted label
@PluginDoc(
*/
class NaiveBayesPredictPlugin extends SparkCommandPlugin[NaiveBayesPredictArgs, FrameEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: NaiveBayesPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: NaiveBayesPredictArgs)(implicit invocation: Invocation): FrameEntity = {
    val models = engine.models
    val frames = engine.frames

    val inputFrame = frames.expectFrame(arguments.frame)
    val modelMeta = models.expectModel(arguments.model)

    //Running MLLib
    val naiveBayesJsObject = modelMeta.data.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
    val naiveBayesData = naiveBayesJsObject.convertTo[NaiveBayesData]
    val naiveBayesModel = naiveBayesData.naiveBayesModel
    if (arguments.observationColumns.isDefined) {
      require(naiveBayesData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }
    val naiveBayesColumns = arguments.observationColumns.getOrElse(naiveBayesData.observationColumns)

    //create RDD from the frame
    val inputFrameRdd = frames.loadFrameData(sc, inputFrame)

    //predicting a label for the observation columns
    val predictionsRDD = inputFrameRdd.mapRows(row => {
      val array = row.valuesAsArray(naiveBayesColumns)
      val doubles = array.map(i => DataTypes.toDouble(i))
      val point = Vectors.dense(doubles)
      val prediction = naiveBayesModel.predict(point)
      row.addValue(DataTypes.toDouble(prediction))
    })

    val updatedSchema = inputFrameRdd.frameSchema.addColumn("predicted_class", DataTypes.float64)
    val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by NaiveBayes predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrameRdd)
    }
  }

}

