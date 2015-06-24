package org.apache.spark.mllib.ia.plugins.classification.glm

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.classification.{ LogisticRegressionWithFrequencyLBFGS, LogisticRegressionModelWithFrequency }
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.{ LabeledPointWithFrequency, GeneralizedLinearAlgorithmWithFrequency }
import org.apache.spark.rdd.RDD

class IaLogisticRegressionModelWithLBFGS() extends IaLogisticRegressionModel {

  val model = new LogisticRegressionWithFrequencyLBFGS()

  /**
   * Get logistic regression model
   */
  override def getModel: LogisticRegressionWithFrequencyLBFGS = model

  /**
   * Get the approximate Hessian matrix at the solution (i.e., final iteration)
   */
  override def getHessianMatrix: Option[DenseMatrix[Double]] = {
    model.optimizer.getHessianMatrix
  }

  /**
   * Create logistic regression model
   *
   * @param arguments model arguments
   * @return Logistic regression model
   */
  override def initialize(arguments: LogisticRegressionTrainArgs): Unit = {
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
