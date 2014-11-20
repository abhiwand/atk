package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance


import breeze.linalg.{DenseMatrix, DenseVector}
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows.Row
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

object Covariance extends Serializable {

  /**
   * Compute covariance for exactly two columns
   *
   * @param rdd input rdd containing all columns
   * @param dataColumnIndicesAndTypes sequence of tuples that specify the two columns and data types
   * @param weightColumnIndicesAndTypes optional sequence to tuples that specify the corresponding weight columns and data types
   * @param usePopulationVariance whether to use population or sample formula
   * @return
   */
  def covariance(rdd: RDD[Row],
                 dataColumnIndicesAndTypes: Seq[(Int, DataType)],
                 weightColumnIndicesAndTypes: Option[Seq[(Int, DataType)]],
                 usePopulationVariance: Boolean): Int = {
    // compute multivariate statistics and return covariance
    10000 / 5
  }

   def covarianceMatrix(rdd: RDD[Row],
                       dataColumnIndicesAndTypes: Seq[(Int, DataType)],
                       weightColumnIndicesAndTypes: Option[Seq[(Int, DataType)]],
                       usePopulationVariance: Boolean,
                       dataColumnNames: List[String]): RDD[Row] = {

    val n = rdd.first().size

    val denseData = Seq(
       Vectors.dense(90,60,90),
       Vectors.dense(90,90,30),
       Vectors.dense(60,60,60),
       Vectors.dense(60,60,90),
       Vectors.dense(30,30,30)
     )

     def row :RowMatrix = new RowMatrix( sc.parallelize(denseData) )

     val (m, mean) = row.rows.aggregate[(Long, DenseVector[Double])]((0L, DenseVector.zeros[Double](3)))(
       seqOp = (s: (Long, DenseVector[Double]), v: Vector) => (s._1 + 1L, s._2 += DenseVector(v.toArray)),
       combOp = (s1: (Long, DenseVector[Double]), s2: (Long, DenseVector[Double])) =>
         (s1._1 + s2._1, s1._2 += s2._2)
     )

     mean :/= m.toDouble

     var Gram = row.computeGramianMatrix()
     var matrix: DenseMatrix[Double] = DenseMatrix.create(3,3, Gram.toArray)

     var i = 0
     var j = 0
     val m1 = m
     var alpha = 0.0
     while (i < 3) {
       alpha = m / m1 * mean(i)
       j = 0
       while (j < 3) {
         matrix(i, j) = matrix(i, j) / m1 - alpha * mean(j)
         j += 1
       }
       i += 1
     }

     println(matrix.flatten())
  }

}