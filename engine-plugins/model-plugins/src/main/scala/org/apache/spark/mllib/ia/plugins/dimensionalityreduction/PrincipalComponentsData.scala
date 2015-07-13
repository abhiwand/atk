package org.apache.spark.mllib.ia.plugins.dimensionalityreduction

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Matrix
/**
 * Command for loading model data into existing model in the model database.
 * @param k
 * @param observationColumns Handle to the observation columns of the data frame
 * @param singularValues
 * @param vFactor
 */
case class PrincipalComponentsData(k: Int,
                                   observationColumns: List[String],
                                   singularValues: Vector,
                                   vFactor: Matrix) {
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumns must not be null nor empty")
  require(k >= 1, "number of Eigen values to use must be greater than equal to 1")
}

