package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance


import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows.Row
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types.DataType

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

     val dataRDD: RDD[Vector] = rdd.map(_).to[Vector]


    /*val (m, mean) = rdd.aggregate[(Long, Array)]((0L, Array(n) = 0.0)
      ( (s: (Long, Array), v: RDD[Row]) => (s._1 + 1L, s._2[0] += v[]),
        (s1: (Long, Array), s2: (Long, Array)) =>  (s1._1 + s2._1, s1._2 += s2._2)
    ))*/




   // rdd.sparkContext.parallelize(covMatrix)
  }

}