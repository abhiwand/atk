/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics

import com.intel.intelanalytics.domain.frame.{ ClassificationMetricArgs, ClassificationMetricValue }
import com.intel.intelanalytics.engine.plugin.{ Invocation, PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes Model accuracy, precision, recall, confusion matrix and f_measure
 *
 * Parameters
 * ----------
 * label_column : str
 * The name of the column containing the correct label for each instance.
 * pred_column : str
 * The name of the column containing the predicted label for each instance.
 * pos_label : [ str | int | Null ] (optional)
 * This is a str or int for binary classifiers, and Null for multi-class
 * classifiers.
 * The value to be interpreted as a positive instance.
 * beta : double (optional)
 * This is the beta value to use for :math:`F_{ \beta}` measure (default F1
 * measure is computed); must be greater than zero.
 * Defaults to 1.
 */
@PluginDoc(oneLine = "Model statistics of accuracy, precision, and others.",
  extended = """Calculate the accuracy, precision, confusion_matrix, recall and
:math:`F_{ \beta}` measure for a classification model.

*   The **f_measure** result is the :math:`F_{ \beta}` measure for a
    classification model.
    The :math:`F_{ \beta}` measure of a binary classification model is the
    harmonic mean of precision and recall.
    If we let:

    * beta :math:`\equiv \beta`,
    * :math:`T_{P}` denote the number of true positives,
    * :math:`F_{P}` denote the number of false positives, and
    * :math:`F_{N}` denote the number of false negatives

    then:

    .. math::

        F_{ \beta} = (1 + \beta ^ 2) * \frac{ \frac{T_{P}}{T_{P} + F_{P}} * \
        \frac{T_{P}}{T_{P} + F_{N}}}{ \beta ^ 2 * \frac{T_{P}}{T_{P} + \
        F_{P}}  + \frac{T_{P}}{T_{P} + F_{N}}}

    The :math:`F_{ \beta}` measure for a multi-class classification model is
    computed as the weighted average of the :math:`F_{ \beta}` measure for
    each label, where the weight is the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **recall** result of a binary classification model is the proportion
    of positive instances that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives and
    :math:`F_{N}` denote the number of false negatives, then the model
    recall is given by :math:`\frac {T_{P}} {T_{P} + F_{N}}`.

    For multi-class classification models, the recall measure is computed as
    the weighted average of the recall for each label, where the weight is
    the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **precision** of a binary classification model is the proportion of
    predicted positive instances that are correct.
    If we let :math:`T_{P}` denote the number of true positives and
    :math:`F_{P}` denote the number of false positives, then the model
    precision is given by: :math:`\frac {T_{P}} {T_{P} + F_{P}}`.

    For multi-class classification models, the precision measure is computed
    as the weighted average of the precision for each label, where the
    weight is the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **accuracy** of a classification model is the proportion of
    predictions that are correct.
    If we let :math:`T_{P}` denote the number of true positives,
    :math:`T_{N}` denote the number of true negatives, and :math:`K` denote
    the total number of classified instances, then the model accuracy is
    given by: :math:`\frac{T_{P} + T_{N}}{K}`.

    This measure applies to binary and multi-class classifiers.

*   The **confusion_matrix** result is a confusion matrix for a
    binary classifier model, formatted for human readability.

Notes
-----
The **confusion_matrix** is not yet implemented for multi-class classifiers.""",
  returns = """object
<object>.accuracy : double
<object>.confusion_matrix : table
<object>.f_measure : double
<object>.precision : double
<object>.recall : double""")
class ClassificationMetricsPlugin extends SparkCommandPlugin[ClassificationMetricArgs, ClassificationMetricValue] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/classification_metrics"
  /**
   * Set the kryo class to use
   */
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: ClassificationMetricArgs)(implicit invocation: Invocation) = 8
  /**
   * Computes Model accuracy, precision, recall, confusion matrix and f_measure
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationMetricArgs)(implicit invocation: Invocation): ClassificationMetricValue = {
    // dependencies (later to be replaced with dependency injection)

    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef = arguments.frame
    val frameEntity = frames.expectFrame(arguments.frame)
    val frameSchema = frameEntity.schema
    val frameRdd = frames.loadLegacyFrameRdd(sc, frameRef)
    val betaValue = arguments.beta.getOrElse(1.0)
    val labelColumnIndex = frameSchema.columnIndex(arguments.labelColumn)
    val predColumnIndex = frameSchema.columnIndex(arguments.predColumn)

    // check if poslabel is an Int, string or None
    val metricsPoslabel: String = arguments.posLabel.isDefined match {
      case false => null
      case true => arguments.posLabel.get match {
        case Left(x) => x
        case Right(x) => x.toString
      }
    }
    // run the operation and return the results
    if (metricsPoslabel == null) {
      ClassificationMetrics.multiclassClassificationMetrics(frameRdd, labelColumnIndex, predColumnIndex, betaValue)
    }
    else {
      ClassificationMetrics.binaryClassificationMetrics(frameRdd, labelColumnIndex, predColumnIndex, metricsPoslabel, betaValue)
    }

  }
}
