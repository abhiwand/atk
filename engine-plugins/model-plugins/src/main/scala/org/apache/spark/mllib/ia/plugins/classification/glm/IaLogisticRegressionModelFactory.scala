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
  def createModel(arguments: LogisticRegressionTrainArgs): IaLogisticRegressionModel = {
    val regressionModel = arguments.optimizer.get.toUpperCase() match {
      case "LBFGS" => new IaLogisticRegressionModelWithLBFGS()
      case "SGD" => new IaLogisticRegressionModelWithSGD()
      case _ => throw new IllegalArgumentException("Only LBFGS or SGD optimizers permitted")
    }
    regressionModel.initialize(arguments)
    regressionModel
  }

}
