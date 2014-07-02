package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.elements.GraphElement

object FeatureProbability {

  /**
   * Parse feature probabilities
   */
  def parseGraphElement(graphElement: GraphElement,
                        priorPropertyName: String,
                        posteriorPropertyName: Option[String],
                        splitPropertyName: Option[String]): List[FeatureProbability] = {

    val priorVector = parseVector(graphElement, priorPropertyName)
    val splitType = parseSplitType(graphElement, splitPropertyName)

    val posteriorVector = if (posteriorPropertyName != None) {
      Some(parseVector(graphElement, posteriorPropertyName.get))
    }
    else {
      None
    }
    parseFeatureProbability(priorVector, posteriorVector, splitType)
  }

  def parseFeatureProbability(priorVector: Array[Double],
                              posteriorVector: Option[Array[Double]],
                              splitType: Option[String]): List[FeatureProbability] = {

    val featureProbabilities = List[FeatureProbability]()
    val featureSize = priorVector.size

    require(posteriorVector == None || posteriorVector.get.size == featureSize)

    var j = 0l
    for (j <- 0 until priorVector.size) {
      val posteriorProbability = if (posteriorVector != None) Some(posteriorVector.get(j)) else None
      featureProbabilities :+ FeatureProbability(j, priorVector(j), posteriorProbability, splitType)
    }
    featureProbabilities
  }

  /**
   * Parse split type
   */
  def parseSplitType(graphElement: GraphElement, splitPropertyName: Option[String]): Option[String] = {
    if (splitPropertyName != None) {
      val splitProperty = graphElement.getProperty(splitPropertyName.get)
      if (splitProperty != None) Some(splitProperty.get.value.toString.toUpperCase) else None
    }
    else {
      None
    }
  }

  /**
   * Parse delimited list of feature probabilities
   *
   * @param graphElement Graph element which can be a vertex or edge
   * @param propertyName Property containing delimited list of feature probabilities
   * @param sep Delimiter (defaults to comma)
   * @return Array of feature probabilities
   */
  def parseVector(graphElement: GraphElement, propertyName: String, sep: String = ","): Array[Double] = {
    val property = graphElement.getProperty(propertyName)
    if (property != None) {
      property.get.value.toString.split(sep).map(parseDouble(_).getOrElse(0d))
    }
    else {
      Array.empty[Double]
    }
  }

  /**
   * Parse double from string
   */
  def parseDouble(str: String): Option[Double] = try {
    Some(str.toDouble)
  }
  catch {
    case e: Exception => None //TODO: Warn?
  }

  def calcRoc(rocFeature: (FeatureProbability, RocParams)): RocCounts = {
    val rocArg = rocFeature._2
    val rocBin = new RocCounts(rocArg.rocSize)
    val prior = rocFeature._1.priorProbability
    val posterior = rocFeature._1.posteriorProbability.get
    var k = 0
    for (k <- 0 until rocArg.rocSize) {
      val tmp = BigDecimal(rocArg.rocMin + rocArg.rocStep * k).setScale(3, BigDecimal.RoundingMode.HALF_UP).toFloat
      val rocThreshold = if (tmp > rocArg.rocMax) rocArg.rocMax else tmp
      if (prior >= rocThreshold) {
        rocBin.numPositives(k) += 1
        if (posterior >= rocThreshold) {
          rocBin.numTruePositives(k) += 1
        }
      }
      else {
        rocBin.numNegatives(k) += 1
        if (posterior >= rocThreshold) {
          rocBin.numFalsePositives(k) += 1
        }
      }
    }
    rocBin
  }

  def updateRoc(rocCounts: RocCounts, rocParams: RocParams): List[Roc] = {
    val rocList = List[Roc]()
    var k = 0
    for (k <- 0 until rocCounts.numTruePositives.length) {
      val threshold = BigDecimal(rocParams.rocMin + rocParams.rocStep * k).setScale(3, BigDecimal.RoundingMode.HALF_UP).toFloat
      val fpr = if (rocCounts.numNegatives(k) > 0) rocCounts.numFalsePositives(k) / rocCounts.numNegatives(k).toFloat else 0f
      val tpr = if (rocCounts.numPositives(k) > 0) rocCounts.numTruePositives(k) / rocCounts.numPositives(k).toFloat else 0f
      rocList :+ Roc(threshold, fpr, tpr)
    }
    rocList :+ Roc(0, 0, 0)
    rocList :+ Roc(1, 1, 1)
    rocList.sortBy(_.fpr).toList
  }
}

case class FeatureProbability(featureId: Long, priorProbability: Double, posteriorProbability: Option[Double], splitType: Option[String]) extends Serializable
