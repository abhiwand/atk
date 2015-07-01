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
package org.apache.spark.mllib.ia.plugins.classification.glm

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
    val regressionModel = arguments.optimizer.toUpperCase match {
      case "LBFGS" => new IaLogisticRegressionModelWithLBFGS()
      case "SGD" => new IaLogisticRegressionModelWithSGD()
      case _ => throw new IllegalArgumentException("Only LBFGS or SGD optimizers permitted")
    }
    regressionModel.initialize(arguments)
    regressionModel
  }

}
