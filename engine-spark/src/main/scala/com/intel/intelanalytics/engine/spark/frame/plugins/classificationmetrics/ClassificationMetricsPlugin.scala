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
import com.intel.intelanalytics.domain.frame.{ ClassificationMetricArgs, ClassificationMetricValue, FrameEntity }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.PythonRDDStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes Model accuracy, precision, recall, confusion matrix and f_measure
 */
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
  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Computes Model accuracy.",
    extendedSummary = Some("""
                           |    Extended Summary
                           |    ----------------
                           |    Calculate the accuracy, precision, confusion_matrix, recall and
                           |    :math:`F_{\\beta}` measure for a classification model.
                           |
                           |    *   The **f_measure** result is the :math:`F_{\\beta}` measure for a
                           |        classification model.
                           |        The :math:`F_{\\beta}` measure of a binary classification model is the
                           |        harmonic mean of precision and recall.
                           |        If we let:
                           |
                           |        * beta :math:`\\equiv \\beta`,
                           |        * :math:`T_{P}` denote the number of true positives,
                           |        * :math:`F_{P}` denote the number of false positives, and
                           |        * :math:`F_{N}` denote the number of false negatives
                           |
                           |        then:
                           |
                           |        .. math::
                           |
                           |            F_{\\beta} = (1 + \\beta ^ 2) * \\frac{\\frac{T_{P}}{T_{P} + \\
                           |            F_{P}} * \\frac{T_{P}}{T_{P} + F_{N}}}{\\beta ^ 2 * \\
                           |            \\frac{T_{P}}{T_{P} + F_{P}} + \\frac{T_{P}}{T_{P} + F_{N}} }
                           |
                           |        The :math:`F_{\\beta}` measure for a multi-class classification model is
                           |        computed as the weighted average of the :math:`F_{\\beta}` measure for
                           |        each label, where the weight is the number of instances of each label.
                           |        The determination of binary vs. multi-class is automatically inferred
                           |        from the data.
                           |
                           |    *   The **recall** result of a binary classification model is the proportion
                           |        of positive instances that are correctly identified.
                           |        If we let :math:`T_{P}` denote the number of true positives and
                           |        :math:`F_{N}` denote the number of false negatives, then the model
                           |        recall is given by: :math:`\\frac {T_{P}} {T_{P} + F_{N}}`.
                           |
                           |        For multi-class classification models, the recall measure is computed as
                           |        the weighted average of the recall for each label, where the weight is
                           |        the number of instances of each label.
                           |        The determination of binary vs. multi-class is automatically inferred
                           |        from the data.
                           |
                           |    *   The **precision** of a binary classification model is the proportion of
                           |        predicted positive instances that are correct.
                           |        If we let :math:`T_{P}` denote the number of true positives and
                           |        :math:`F_{P}` denote the number of false positives, then the model
                           |        precision is given by: :math:`\\frac {T_{P}} {T_{P} + F_{P}}`.
                           |
                           |        For multi-class classification models, the precision measure is computed
                           |        as the weighted average of the precision for each label, where the
                           |        weight is the number of instances of each label.
                           |        The determination of binary vs. multi-class is automatically inferred
                           |        from the data.
                           |
                           |    *   The **accuracy** of a classification model is the proportion of
                           |        predictions that are correct.
                           |        If we let :math:`T_{P}` denote the number of true positives,
                           |        :math:`T_{N}` denote the number of true negatives, and :math:`K` denote
                           |        the total number of classified instances, then the model accuracy is
                           |        given by: :math:`\\frac{T_{P} + T_{N}}{K}`.
                           |
                           |        This measure applies to binary and multi-class classifiers.
                           |
                           |    *   The **confusion_matrix** result is a confusion matrix for a
                           |        binary classifier model, formatted for human readability.
                           |        See notes below.
                           |
                           |    Parameters
                           |    ----------
                           |    label_column : str
                           |        the name of the column containing the correct label for each
                           |        instance.
                           |
                           |    pred_column : str
                           |        the name of the column containing the predicted label for each
                           |        instance.
                           |
                           |    pos_label : [ str | int | Null ] (optional)
                           |        str or int for binary classifiers, Null for multi-class classifiers.
                           |        The value to be interpreted as a positive instance.
                           |
                           |    beta : double (optional)
                           |        beta value to use for :math:`F_{\\beta}` measure (default F1 measure
                           |        is computed); must be greater than zero.
                           |        Defaults to 1.
                           |
                           |    Notes
                           |    -----
                           |    **confusion_matrix** is not yet implemented for multi-class classifiers
                           |
                           |    Returns
                           |    -------
                           |    object
                           |    <object>.accuracy : double
                           |    <object>.confusion_matrix : table
                           |    <object>.f_measure : double
                           |    <object>.precision : double
                           |    <object>.recall : double
                           |        
                           |    Examples
                           |    --------
                           |    Consider the following sample data set in *frame* with actual data
                           |    labels specified in the *labels* column and the predicted labels in the
                           |    *predictions* column::
                           |
                           |        frame.inspect()
                           |
                           |          a:unicode   b:int32   labels:int32  predictions:int32
                           |        /-------------------------------------------------------/
                           |            red         1              0                  0
                           |            blue        3              1                  0
                           |            blue        1              0                  0
                           |            green       0              1                  1
                           |
                           |        cm = frame.classification_metrics('labels', 'predictions', 1, 1)
                           |
                           |        cm.f_measure
                           |
                           |        0.66666666666666663
                           |
                           |        cm.recall
                           |
                           |        0.5
                           |
                           |        cm.accuracy
                           |
                           |        0.75
                           |
                           |        cm.precision
                           |
                           |        1.0
                           |
                           |        cm.confusion_matrix
                           |
                           |                      Predicted
                           |                     _pos_ _neg__
                           |        Actual  pos |  1     1
                           |                neg |  0     2
                           |
                           |
                           |    .. versionchanged:: 0.8
                           |
                            """.stripMargin)))

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
    val ctx = sc

    // validate arguments
    val frameId = arguments.frame.id
    val frameEntity = frames.expectFrame(arguments.frame)
    val frameSchema = frameEntity.schema
    val frameRdd = frames.loadLegacyFrameRdd(ctx, frameId)
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
