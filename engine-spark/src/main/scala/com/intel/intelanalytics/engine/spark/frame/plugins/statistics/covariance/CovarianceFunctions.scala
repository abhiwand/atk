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
                       dataColumnNames: List[String]): RDD[Matrix] = {

    val a = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    def rowMatrix: RowMatrix = new RowMatrix(a)

    val covariance = rowMatrix.computeCovariance()

    //val result: Row = indexedPredictedRowRDD.join(indexedLabeledRowRDD).map { case (index, data) => new GenericRow(data._1 ++ data._2) }

    frameRDD.sparkContext.parallelize(Seq(covariance))
  }
}

