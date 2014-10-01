//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ ClassificationMetric, ClassificationMetricValue, DataFrame }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes Model accuracy, precision, recall, confusion matrix and f_measure
 */
class ClassificationMetricsPlugin extends SparkCommandPlugin[ClassificationMetric, ClassificationMetricValue] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/classification_metrics"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Computes Model accuracy, precision, recall, confusion matrix and f_measure (math:`F_{\\beta}`).",
    extendedSummary = Some("""
    Based on the *metric_type* argument provided, it computes the accuracy, precision, recall or :math:`F_{\\beta}` measure for a classification model

    --- When metric_type provided is 'f_measure': Computes the :math:`F_{\\beta}` measure for a classification model.
    A column containing the correct labels for each instance and a column containing the predictions made by the model are specified.
    The :math:`F_{\\beta}` measure of a binary classification model is the harmonic mean of precision and recall.
    If we let:

    * beta :math:`\\equiv \\beta`,
    * :math:`T_{P}` denote the number of true positives,
    * :math:`F_{P}` denote the number of false positives, and
    * :math:`F_{N}` denote the number of false negatives,

    then:
    .. math::
      F_{\\beta} = \\left(1 + \\beta ^ 2\\right) * \\frac{\\frac{T_{P}}{T_{P} + F_{P}} * \\frac{T_{P}}{T_{P} + F_{N}}}{\\beta ^ 2 * \\
      \\left(\\frac{T_{P}}{T_{P} + F_{P}} + \\frac{T_{P}}{T_{P} + F_{N}}\\right)}

    For multi-class classification, the :math:`F_{\\beta}` measure is computed as the weighted average of the :math:`F_{\\beta}` measure
    for each label, where the weight is the number of instance with each label in the labeled column.  The
    determination of binary vs. multi-class is automatically inferred from the data.

    --- When metric_type provided is 'recall': Computes the recall measure for a classification model.
    A column containing the correct labels for each instance and a column containing the predictions made by the model are specified.
    The recall of a binary classification model is the proportion of positive instances that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives and :math:`F_{N}` denote the number of false
    negatives, then the model recall is given by: :math:`\\frac {T_{P}} {T_{P} + F_{N}}`.

    For multi-class classification, the recall measure is computed as the weighted average of the recall
    for each label, where the weight is the number of instance with each label in the labeled column.  The
    determination of binary vs. multi-class is automatically inferred from the data.

    --- When metric_type provided is 'precision': Computes the precision measure for a classification model
    A column containing the correct labels for each instance and a column containing the predictions made by the
    model are specified.  The precision of a binary classification model is the proportion of predicted positive
    instances that are correct.  If we let :math:`T_{P}` denote the number of true positives and :math:`F_{P}` denote the number of false
    positives, then the model precision is given by: :math:`\\frac {T_{P}} {T_{P} + F_{P}}`.

    For multi-class classification, the precision measure is computed as the weighted average of the precision
    for each label, where the weight is the number of instances with each label in the labeled column.  The
    determination of binary vs. multi-class is automatically inferred from the data.

    --- When metric_type provided is 'accuracy': Computes the accuracy measure for a classification model
    A column containing the correct labels for each instance and a column containing the predictions made by the classifier are specified.
    The accuracy of a classification model is the proportion of predictions that are correct.
    If we let :math:`T_{P}` denote the number of true positives, :math:`T_{N}` denote the number of true negatives, and :math:`K`
    denote the total number of classified instances, then the model accuracy is given by: :math:`\\frac{T_{P} + T_{N}}{K}`.

    This measure applies to binary and multi-class classifiers.


    Parameters
    ----------
    metric_type : str
      the model that is to be computed
    label_column : str
      the name of the column containing the correct label for each instance
    pred_column : str
      the name of the column containing the predicted label for each instance
    pos_label : str
      the value to be interpreted as a positive instance (only for binary, ignored for multi-class)
    beta : float
      beta value to use for :math:`F_{\\beta}` measure (default F1 measure is computed); must be greater than zero

    Returns
    -------
    float64
    the measure for the classifier

    Examples
    --------
    Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
    the predicted labels in the *predictions* column::

    frame.inspect()

    a:unicode   b:int32   labels:int32  predictions:int32
                             |-------------------------------------------------------|
    red               1              0                  0
    blue              3              1                  0
    blue              1              0                  0
    green             0              1                  1

    frame.classification_metrics('f_measure', 'labels', 'predictions', '1', 1)

    0.66666666666666663

    frame.classification_metrics('f_measure', 'labels', 'predictions', '1', 2)

    0.55555555555555558

    frame.classification_metrics('f_measure', 'labels', 'predictions', '0', 1)

    0.80000000000000004


    frame.classification_metrics('recall', 'labels', 'predictions', '1', 1)

    0.5

    frame.classification_metrics('recall', 'labels', 'predictions', '0', 1)

    1.0


    frame.classification_metrics('precision', 'labels', 'predictions', '1', 1)

    1.0

    frame.classification_metrics('precision', 'labels', 'predictions', '0', 1)

    0.66666666666666663


    frame.classification_metrics('accuracy', 'labels', 'predictions', '1', 1)

    0.75

    frame.classification_metrics('confusion_matrix', 'labels', 'predictions', '1', 1)

    [1, 2, 0, 1]


    .. versionadded:: 0.8  """)))

  /**
   * Computes Model accuracy, precision, recall, confusion matrix and f_measure
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ClassificationMetric)(implicit user: UserPrincipal, executionContext: ExecutionContext): ClassificationMetricValue = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.frame.id
    val frameMeta: DataFrame = frames.expectFrame(frameId)
    val frameSchema = frameMeta.schema
    val frameRdd = frames.loadFrameRdd(ctx, frameId)
    val labelColumnIndex = frameSchema.columnIndex(arguments.labelColumn)
    val predColumnIndex = frameSchema.columnIndex(arguments.predColumn)

    // run the operation and return the results
    if (arguments.metricType == "confusion_matrix") {
      val valueList = ClassificationMetrics.confusionMatrix(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
      ClassificationMetricValue(None, Some(valueList))
    }
    else {
      val metric_value = arguments.metricType match {
        case "accuracy" => ClassificationMetrics.modelAccuracy(frameRdd, labelColumnIndex, predColumnIndex)
        case "precision" => ClassificationMetrics.modelPrecision(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
        case "recall" => ClassificationMetrics.modelRecall(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel)
        case "f_measure" => ClassificationMetrics.modelFMeasure(frameRdd, labelColumnIndex, predColumnIndex, arguments.posLabel, arguments.beta)
        case invalid: String => throw new IllegalArgumentException(s"Invalid metric type: $invalid, valid values include: confusion_matrix, accuracy, precision, recall, f_measure")
      }
      ClassificationMetricValue(Some(metric_value), None)
    }

  }
}
