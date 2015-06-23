package org.apache.spark.mllib.ia.plugins.classification.glm

import org.apache.spark.mllib.classification.{ LogisticRegressionModelWithFrequency, LogisticRegressionWithFrequencySGD }
import org.apache.spark.mllib.optimization.{ L1Updater, SquaredL2Updater }
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency

class IaLogisticRegressionModelWithSGD extends IaLogisticRegressionModel {

  /**
   * Create logistic regression model
   *
   * @param arguments model arguments
   * @return Logistic regression model
   */
  override def createModel(arguments: LogisticRegressionTrainArgs): GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency] = {

    val model = new LogisticRegressionWithFrequencySGD()
    model.optimizer.setNumIterations(arguments.getNumIterations)
    model.optimizer.setStepSize(arguments.getStepSize)
    model.optimizer.setRegParam(arguments.getRegParam)
    model.optimizer.setMiniBatchFraction(arguments.getMiniBatchFraction)
    model.setFeatureScaling(arguments.getFeatureScaling)

    if (arguments.regType.isDefined) {
      model.optimizer.setUpdater(arguments.regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }

    model.setIntercept(arguments.getIntercept)
  }
}
