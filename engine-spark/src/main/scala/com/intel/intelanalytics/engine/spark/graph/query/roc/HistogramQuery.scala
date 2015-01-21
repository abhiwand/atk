package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.elements.{ GBEdge, GBVertex }
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.storage.StorageLevel

import scala.concurrent._
import com.intel.intelanalytics.domain.command.CommandDoc

/*
 * TODO: The ROC code does not compute the standard ROC curve per 
 * http://en.wikipedia.org/wiki/Receiver_operating_characteristic
 * It implements something else specific to one of our internal POC engagements.
 * 
 * It must be corrected before being re-enabled. Alternativeley we replace this function entirely.
 */

/**
 * Get histogram and optionally ROC curve on property values
 *
 * @param graph The graph reference
 * @param prior_property_list The property name on which users want to get histogram.
 *                            When used without second_property_name, this property name can from either prior
 *                            or posterior properties. When used together with second_property_name, expect the
 *                            first_property_name is from prior properties, and the second_property_name is from
 *                            posterior properties.
 *
 * @param posterior_property_list The property name on which users want to get histogram.
 *                                The default value is empty string.
 * @param property_type  The type of the first and second property.
 *                       Valid values are either VERTEX_PROPERTY or EDGE_PROPERTY.
 *                       The default value is VERTEX_PROPERTY
 * @param vertex_type_property_key The property name for vertex type. The default value "vertex_type".
 *                                 We need this name to know data is in train, validation or test splits
 * @param split_types The list of split types to include in the report.
 *                    A semi-colon separated string with train (TR), validation (VA), and test (TE) splits.
 *                    The default value is "TR;VA;TE"
 *
 * @param histogram_buckets The number of buckets to plot histogram. The default value is 30.
 */
case class HistogramParams(graph: GraphReference,
                           prior_property_list: String,
                           posterior_property_list: Option[String],
                           // enable_roc: Option[Boolean],
                           // roc_threshold: Option[List[Double]],
                           property_type: Option[String],
                           vertex_type_property_key: Option[String],
                           split_types: Option[List[String]],
                           histogram_buckets: Option[Int]) {
  // require(roc_threshold == None|| roc_threshold.get.size == 3, "Please input roc_threshold using [min, step, max] format")
}

/**
 * Algorithm report comprising of histogram of prior and posterior probabilities, and ROC curves.
 *
 * @param prior_histograms Histograms of prior probabilities (one for each feature)
 * @param posterior_histograms Histograms of posterior probabilities (one for each feature)
 */
case class HistogramResult(prior_histograms: List[Histogram],
                           posterior_histograms: Option[List[Histogram]])

/** JSON conversion */
object HistogramJsonFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val histogramParamsFormat = jsonFormat7(HistogramParams)
  // implicit val rocFormat = jsonFormat3(Roc)
  // implicit val rocCurveFormat = jsonFormat2(RocCurve)
  implicit val histogramFormat = jsonFormat2(Histogram.apply)
  implicit val histogramResultFormat = jsonFormat2(HistogramResult)
}

import HistogramJsonFormat._

class HistogramQuery extends SparkCommandPlugin[HistogramParams, HistogramResult] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   */
  override def name: String = "graph:titan/query/histogram"

  override def execute(arguments: HistogramParams)(implicit invocation: Invocation): HistogramResult = {
    import scala.concurrent.duration._

    System.out.println("*********In Execute method of Histogram query********")
    val config = configuration
    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //val defaultRocParams = config.getDoubleList("roc-threshold").asScala.toList.map(_.doubleValue())
    //val rocParams = RocParams(arguments.roc_threshold.getOrElse(defaultRocParams))

    // Specifying variable type due to Scala bug
    // val enableRoc = arguments.enable_roc.getOrElse(config.getBoolean("enable-roc"))
    val numBuckets = arguments.histogram_buckets.getOrElse(config.getInt("histogram-buckets"))
    val propertyClass = arguments.property_type match {
      case Some("EDGE_PROPERTY") => classOf[GBEdge]
      case Some("VERTEX_PROPERTY") => classOf[GBVertex]
      case None => classOf[GBVertex]
      case _ => throw new IllegalArgumentException("Please input 'VERTEX_PROPERTY' or 'EDGE_PROPERTY' for property_type")
    }

    // Create graph connection
    val graphElementRDD = if (propertyClass.isInstanceOf[GBEdge]) {
      engine.graphs.loadGbEdges(sc, graph)
    }
    else engine.graphs.loadGbVertices(sc, graph)

    // Parse features
    val featureVectorRDD = graphElementRDD.map(element => {
      FeatureVector.parseGraphElement(element, arguments.prior_property_list, arguments.posterior_property_list, arguments.vertex_type_property_key)
    })

    // Filter by split type
    val filteredFeatureRDD = if (arguments.split_types != None) {
      featureVectorRDD.filter(f => arguments.split_types.get.contains(f.splitType))
    }
    else featureVectorRDD
    filteredFeatureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // Compute histograms
    val priorHistograms = FeatureVector.getHistograms(filteredFeatureRDD, usePosterior = false, numBuckets)
    val posteriorHistograms = if (arguments.posterior_property_list != None) {
      Some(FeatureVector.getHistograms(filteredFeatureRDD, usePosterior = true, numBuckets))
    }
    else None

    filteredFeatureRDD.unpersist()

    HistogramResult(priorHistograms, posteriorHistograms)

  }
}
