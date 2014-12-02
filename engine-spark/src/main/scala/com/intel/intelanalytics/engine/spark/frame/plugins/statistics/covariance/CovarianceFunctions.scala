package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import breeze.linalg.DenseVector
import breeze.linalg.{ DenseMatrix, DenseVector }
import com.intel.intelanalytics.domain.frame.CovarianceReturn
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
                 dataColumnNames: List[String]): CovarianceReturn = {
    // compute multivariate statistics and return covariance

    val a = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    val q = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      b(0) * b(1)
    })

    def rowMatrix: RowMatrix = new RowMatrix(a)

    //
    val (m, mean) = rowMatrix.rows.aggregate[(Long, DenseVector[Double])]((0L, DenseVector.zeros[Double](a.first().size)))(
      seqOp = (s: (Long, DenseVector[Double]), v: Vector) => (s._1 + 1L, s._2 += DenseVector(v.toArray)),
      combOp = (s1: (Long, DenseVector[Double]), s2: (Long, DenseVector[Double])) =>
        (s1._1 + s2._1, s1._2 += s2._2)
    )
    mean :/= m.toDouble

    print("mean0:" + mean(0))
    print("mean1" + mean(1))

    val product = rowMatrix.rows.aggregate[Double](0)((s: Double, v: Vector) => {
      val d = v.toArray
      d(0) * d(1)
    }, combOp = (s1: Double, s2: Double) => (s1 + s2))
    print("mean1" + product)
    val covariance = product / m - 1 - (mean(0) * mean(1) * m / m - 1)
    CovarianceReturn(covariance)
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
    val rowOrCols = covariance.numCols //it is a square matrix

    //now convert the covariance matrix into a RDD[sql.Row] is there a better way?
    val matrix: DenseMatrix[Double] = DenseMatrix.create(rowOrCols, rowOrCols, covariance.toArray)
    var i, j = 0

    val array: Array[GenericRow] = new Array[GenericRow](rowOrCols)
    while (i < rowOrCols) {
      val arrayCols = new Array[Any](rowOrCols)
      j = 0
      while (j < rowOrCols) {
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

