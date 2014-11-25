package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance


import breeze.linalg.DenseVector
import breeze.linalg.{DenseMatrix, DenseVector}
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
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
                       frameRDD: FrameRDD,
                       dataColumnIndicesAndTypes: Seq[(Int, DataType)],
                       weightColumnIndicesAndTypes: Option[Seq[(Int, DataType)]],
                       usePopulationVariance: Boolean,
                       dataColumnNames: List[String]): RDD[Row] = {

    val n = rdd.first().size
    val a = frameRDD.mapRows(row => {
      val array = row.allValuesAsArray()
      val b = array.map( i => DataTypes.toDouble(i))
      Vectors.dense(b)
    }   )
    val test = a.collect()
    var array = test(0).toArray
    var q = 0
    val finalArray = {
      for (q <- test.length-1)
        array = array ++ test(q).toArray
      array
    }

    val weighted = weightColumnIndicesAndTypes.nonEmpty

    var uniform = n

    val weights: Seq[Double] = {
      if (weighted) {
        val w = weightColumnIndicesAndTypes.get
        val meanWeight = w.map(i => i._1).sum.toDouble
        uniform = 1
        w.map(w => w._1/meanWeight)
      }
      else {
        val a = new Array[Double](m.toInt)
        a.map(x => (x + 1.0))
      }
    }

    def row :RowMatrix = new RowMatrix(a)

    val (m, mean) = row.rows.aggregate[(Long, DenseVector[Double])]((0L, DenseVector.zeros[Double](n)))(
      seqOp = (s: (Long, DenseVector[Double]), v: Vector) => (s._1 + 1L, s._2 += DenseVector(v.toArray)),
      combOp = (s1: (Long, DenseVector[Double]), s2: (Long, DenseVector[Double])) =>
        (s1._1 + s2._1, s1._2 += s2._2)
    )

    if (weighted)
    {
      var idx = 0
      while (idx < m) {
        mean(idx) *= weights(idx)
        idx += 1
      }
    }
    else
      mean :/= m.toDouble

    val covariance = new DenseMatrix[Double](n, n)
    val y = new DenseMatrix[Double](n, m.toInt, finalArray)
    var i = 0
    var j = 0
    for(i <- n )
    {
      for(j <- n)
      {
        covariance(i,j)=0;
        for( k <- m)
        {
          covariance(i,j)= covariance(i,j) + ( weights(k) * y(i, k) * y(j, k))
        }
        //calculate the covariance
        covariance(i, j ) = covariance(i, j) / uniform- mean(i) * mean(j)
      }
    }

   /*  val Gram = row.computeGramianMatrix()
       val matrix: DenseMatrix[Double] = DenseMatrix.create(n,n, Gram.toArray)

       var xi = 0
       var xj = 0
       val m1 = m
       var alpha = 0.0
       while (xi < n) {
         alpha = m / m1 * mean(xi)
         xj = 0
         while (xj < n) {
           matrix(xi, xj) = matrix(xi, xj) / m1 - alpha * mean(xj)
           xj += 1
         }
         xi += 1
       }
  */
    rdd.sparkContext.parallelize(Seq(covariance))
  }

}

