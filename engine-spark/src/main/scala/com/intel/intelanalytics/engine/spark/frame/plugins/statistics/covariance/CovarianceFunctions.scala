package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import breeze.linalg.DenseVector
import breeze.linalg.{ DenseMatrix, DenseVector }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow

object Covariance extends Serializable {

  /**
   * Compute covariance for exactly two columns
   *
   * @param frameRDD input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance
   * @return
   */
  def covariance(frameRDD: FrameRDD,
                 dataColumnNames: List[String]): Int = {
    // compute multivariate statistics and return covariance
    10000 / 5
  }
  /**
   * Compute covariance for two or more columns
   *
   * @param frameRDD input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance matrix
   * @return
   */
  def covarianceMatrix(frameRDD: FrameRDD,
                       dataColumnNames: List[String]): RDD[sql.Row] = {

    val a = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    def rowMatrix: RowMatrix = new RowMatrix(a)

    val covariance = rowMatrix.computeCovariance()
    val matrix: DenseMatrix[Double] = DenseMatrix.create(covariance.numCols, covariance.numCols, covariance.toArray)
    val rows = covariance.numRows
    var i, j = 0

    val array: Array[GenericRow] = new Array[GenericRow](covariance.numRows)
    while (i < rows) {
      val arrayCols = new Array[Any](covariance.numCols)
      j = 0
      while (j < rows) {
        arrayCols(j) = matrix(i, j)
        j += 1
      }
      val genRow = new GenericRow(arrayCols)
      array(i) = genRow
      i += 1
    }

    frameRDD.sparkContext.parallelize(array)
  }
}

