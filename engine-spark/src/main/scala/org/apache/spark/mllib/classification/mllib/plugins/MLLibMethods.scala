package org.apache.spark.mllib.classification.mllib.plugins

import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object MLLibMethods {

  /* This method creates a labeled RDD as required by MLLib */
  def createLabeledRdd(inputRdd: FrameRDD): RDD[LabeledPoint] = {
    val inputSplitRdd: RDD[DenseVector] =
      inputRdd.map { row =>
        new DenseVector(row.map(item => item.asInstanceOf[Double]).toArray)
      }
    inputSplitRdd.map { row =>
      val features = new DenseVector(row.toArray.slice(1, row.size))
      val label = row(0)
      new LabeledPoint(label, features)
    }
  }

  /* This method invokes MLLib's classification metrics. May be replaced with calls to IAT's metrics */
  def outputClassificationMetrics(scoreAndLabelRdd: RDD[(Double, Double)]) = {
    val bcm = new BinaryClassificationMetrics(scoreAndLabelRdd)
    println("\n Area under PR: " + bcm.areaUnderPR())
    println("\n Area under ROC: " + bcm.areaUnderROC())
    val fMeasure = bcm.fMeasureByThreshold()
    println("\n fMeasure: " + fMeasure.collect())
    val precision = bcm.precisionByThreshold()
    println("\n precision: " + precision.collect())
    val recall = bcm.recallByThreshold()
    println("\n recall: "+ recall.collect())
    val roc = bcm.roc()
    println("\n roc: "+ roc.collect())
    val threshold = bcm.thresholds()
    println("\n threshold: "+threshold.collect())
  }
}
