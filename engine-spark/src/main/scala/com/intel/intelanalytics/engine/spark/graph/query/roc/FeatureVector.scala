package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.elements.GraphElement
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer

object FeatureVector {

  /**
   * Parse graph element to obtain prior and posterior feature probabilities
   *
   * @param graphElement Graph element which can be a vertex or an edge
   * @param priorPropertyName Property name for prior probabilities
   * @param posteriorPropertyName  Property name for posterior probabilities
   * @param splitPropertyName  Property name for split type
   * @return Feature probabilities
   */
  def parseGraphElement(graphElement: GraphElement,
                        priorPropertyName: String,
                        posteriorPropertyName: Option[String],
                        splitPropertyName: Option[String]): FeatureVector = {

    val priorArray = parseDoubleArray(graphElement, priorPropertyName, " ")
    val splitType = parseSplitType(graphElement, splitPropertyName)

    val posteriorArray = if (posteriorPropertyName != None) {
      parseDoubleArray(graphElement, posteriorPropertyName.get, ",")
    }
    else Array.empty[Double]

    // Prior and posterior vector should be the same size
    if (posteriorArray.isEmpty || posteriorArray.size == priorArray.size) {
      FeatureVector(priorArray, posteriorArray, splitType)
    }
    else {
      throw new RuntimeException("Prior vector and posterior vector do not have the same length in graph element")
    }
  }

  /**
   * Get split type from graph element.
   *
   * The split type can be any user-defined value, e.g., train (TR), validation (VA), and test (TE).
   *
   * @param graphElement Graph element which can be a vertex or an edge
   * @param splitPropertyName Property name for split type
   * @return An optional split type
   */
  def parseSplitType(graphElement: GraphElement, splitPropertyName: Option[String]): String = {
    val splitType = for {
      name <- splitPropertyName
      property <- graphElement.getProperty(name)
    } yield property.value.toString.toUpperCase
    splitType.getOrElse("")
  }

  /**
   * Parse delimited list of feature probabilities
   *
   * @param graphElement Graph element which can be a vertex or edge
   * @param propertyName Property containing delimited list of feature probabilities
   * @param sep Delimiter (defaults to comma)
   * @return Array of feature probabilities
   */
  def parseDoubleArray(graphElement: GraphElement, propertyName: String, sep: String = " "): Array[Double] = {
    val property = graphElement.getProperty(propertyName)
    if (property != None) {
      property.get.value.toString.split(sep).map(p => {
        try { p.toDouble } catch { case _: Throwable => 0d }
      })
    }
    else {
      throw new RuntimeException("Property does not exist in the graph element: propertyName=" +
        propertyName + ", graph element=" + graphElement  )
    }
  }

  /**
   * Get histograms of feature probabilities
   *
   * @param featureVectorRDD  RDD with feature probabilities
   * @param usePosterior Indicates whether to use posterior or prior probabilities
   * @param numBuckets Number of bins for histogram
   * @return List of histograms
   */
  def getHistograms(featureVectorRDD: RDD[FeatureVector],
                    usePosterior: Boolean = false,
                    numBuckets: Int = 30): List[Histogram] = {
    val histograms = ArrayBuffer[Histogram]()
    val featureSize = featureVectorRDD.first().priorArray.size

    var i = 0
    for (i <- 0 until featureSize) {
      val valueRDD = featureVectorRDD.map(f => {
        if (usePosterior) f.posteriorArray(i) else f.priorArray(i)
      })
      val histogram = Histogram.getHistogram(valueRDD, numBuckets)
      histograms += histogram
    }

    histograms.toList
  }

  /**
   * ROC curves for each feature and split type.
   *
   * @param featureVectorRDD RDD of feature vectors
   * @param rocParams ROC parameters
   *
   * @return List of ROC curves for each feature and split type
   */
  def getRocCurves(featureVectorRDD: RDD[FeatureVector], rocParams: RocParams): List[List[RocCurve]] = {
    val rocCurves = ArrayBuffer[List[RocCurve]]()
    val featureSize = featureVectorRDD.first().priorArray.size

    var i = 0
    for (i <- 0 until featureSize) {
      val rocCountsRDD = featureVectorRDD.map(f =>
        (f.splitType, RocCounts.updateRocCounts(f.priorArray(i), f.posteriorArray(i), rocParams))
      ).reduceByKey((a1, a2) => a1.merge(a2))

      val rocRdd = rocCountsRDD.map(r => (RocCurve(r._1, RocCounts.calcRoc(r._2, rocParams))))
      rocCurves += rocRdd.collect().toList
    }
    rocCurves.toList
  }
}

/**
 * Feature vector containing prior and posterior probabilities.
 *
 * @param priorArray Prior probabilities (one for each feature)
 * @param posteriorArray  Posterior probabilities (one for each feature)
 * @param splitType An split type which can be train (TR), validation (VA), and test (TE)
 */
case class FeatureVector(priorArray: Array[Double], posteriorArray: Array[Double], splitType: String)