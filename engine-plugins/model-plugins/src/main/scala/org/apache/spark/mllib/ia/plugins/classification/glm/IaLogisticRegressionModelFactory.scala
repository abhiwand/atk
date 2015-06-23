package org.apache.spark.mllib.ia.plugins.classification.glm

import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency

object IaLogisticRegressionModelFactory {

  /**
   * Create logistic regression model based on optimizer.
   *
   * The optimizers supported are: LBFGS, and SGD
   *
   * @param arguments Model training arguments
   * @return Logistic regression model
   */
  def createModel(arguments: LogisticRegressionTrainArgs): GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency] = {
    arguments.optimizer.toUpperCase match {
      case "LBFGS" => new IaLogisticRegressionModelWithLBFGS().createModel(arguments)
      case "SGD" => new IaLogisticRegressionModelWithSGD().createModel(arguments)
      case _ => throw new IllegalArgumentException("Only LBFGS or SGD optimizers permitted")
    }
  }

}
