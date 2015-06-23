package org.apache.spark.mllib.ia.plugins.classification.glm

import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency

trait IaLogisticRegressionModel {

  /**
   * Create logistic regression model
   *
   * @param arguments model arguments
   * @return Logistic regression model
   */
  def createModel(arguments: LogisticRegressionTrainArgs): GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency]

}
