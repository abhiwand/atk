package org.apache.spark.mllib.ia.plugins.classification.glm

import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.apache.spark.mllib.evaluation.Hessian
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency

trait IaLogisticRegressionModel extends Hessian {

  /**
   * Initialize logistic regression model
   *
   * @param arguments model arguments
   */
  def initialize(arguments: LogisticRegressionTrainArgs): Unit

  /**
   * Get logistic regression model
   */
  def getModel: GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency]
}
