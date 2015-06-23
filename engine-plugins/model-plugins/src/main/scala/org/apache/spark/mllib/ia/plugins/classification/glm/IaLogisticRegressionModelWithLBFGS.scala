package org.apache.spark.mllib.ia.plugins.classification.glm

import org.apache.spark.mllib.classification.{ LogisticRegressionWithFrequencyLBFGS, LogisticRegressionModelWithFrequency }
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency

class IaLogisticRegressionModelWithLBFGS extends IaLogisticRegressionModel {

  /**
   * Create logistic regression model
   *
   * @param arguments model arguments
   * @return Logistic regression model
   */
  override def createModel(arguments: LogisticRegressionTrainArgs): GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency] = {

    val model = new LogisticRegressionWithFrequencyLBFGS()
    model.optimizer.setNumIterations(arguments.getNumIterations)
    model.optimizer.setConvergenceTol(arguments.getConvergenceTolerance)
    model.optimizer.setNumCorrections(arguments.getNumCorrections)
    model.optimizer.setRegParam(arguments.getRegParam)
    model.setNumClasses(arguments.getNumClasses)
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
