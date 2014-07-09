package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.GraphElement
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.Config
import org.apache.spark.storage.StorageLevel
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.JavaConverters._
import scala.concurrent._

/**
 * Get histogram and optionally ROC curve on property values
 *
 * @param graph The graph reference
 * @param priorPropertyName The property name on which users want to get histogram.
 *                          When used without second_property_name, this property name can from either prior
 *                          or posterior properties. When used together with second_property_name, expect the
 *                          first_property_name is from prior properties, and the second_property_name is from
 *                          posterior properties.
 *
 * @param posteriorPropertyName The property name on which users want to get histogram.
 *                              The default value is empty string.
 * @param enableRoc True means to plot ROC curve on the validation (VA) and test(TE) splits of
 *                  the prior and posterior values, as well as calculate the AUC value on each
 *                  feature dimension of the prior and posterior values.
 *                  False means not to plot ROC curve.
 *                  The default value is False.
 * @param rocThreshold The ROC threshold parameters in "min:step:max" format.
 *                     The default value is "0:0.05:1"
 * @param propertyType  The type of the first and second property.
 *                      Valid values are either VERTEX_PROPERTY or EDGE_PROPERTY.
 *                      The default value is VERTEX_PROPERTY
 * @param vertexTypeKey The property name for vertex type. The default value "vertex_type".
 *                      We need this name to know data is in train, validation or test splits
 * @param splitTypes The list of split types to include in the report.
 *                   A semi-colon separated string with train (TR), validation (VA), and test (TE) splits.
 *                   The default value is "TR;VA;TE"
 *
 * @param numBuckets The number of buckets to plot histogram. The default value is 30.
 */
case class HistogramRocParams(graph: GraphReference,
                              priorPropertyName: String,
                              posteriorPropertyName: Option[String] = Some(""),
                              enableRoc: Option[Boolean] = Some(false),
                              rocThreshold: Option[List[Double]] = Some(List(0, 0.05, 1)),
                              propertyType: Option[String] = Some("VERTEX_PROPERTY"),
                              vertexTypeKey: Option[String] = Some("vertex_type"),
                              splitTypes: Option[List[String]] = Some(List("TR", "VA", "TE")),
                              numBuckets: Option[Int] = Some(30)) {
  require(rocThreshold.get.size == 3)
}

/**
 * Algorithm report comprising of histogram of prior and posterior probabilities, and ROC curves.
 *
 * @param priorHistograms Histograms of prior probabilities (one for each feature)
 * @param posteriorHistograms Histograms of posterior probabilities (one for each feature)
 * @param rocCurves ROC curves for each feature and split type
 */
case class HistogramRocResult(priorHistograms: List[Histogram],
                              posteriorHistograms: Option[List[Histogram]],
                              rocCurves: Option[List[List[RocCurve]]])

class HistogramRocQuery extends SparkCommandPlugin[HistogramRocParams, HistogramRocResult] {
  import DomainJsonProtocol._
  implicit val histogramRocParamsFormat = jsonFormat9(HistogramRocParams)
  implicit val rocFormat = jsonFormat3(Roc)
  implicit val rocCurveFormat = jsonFormat2(RocCurve)
  implicit val histogramFormat = jsonFormat2(Histogram.apply)
  implicit val histogramRocResultFormat = jsonFormat3(HistogramRocResult)

  override def execute(invocation: SparkInvocation, arguments: HistogramRocParams)(implicit user: UserPrincipal, executionContext: ExecutionContext): HistogramRocResult = {
    // TODO: Integrate with Joyesh
    System.out.println("*********In Execute method of LBP********")

    val config = configuration().get
    val graphFuture = invocation.engine.getGraph(arguments.graph.id)

    // Change this to read from default-timeout
    import scala.concurrent.duration._
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    val priorPropertyName = arguments.priorPropertyName
    val posteriorPropertyName = arguments.posteriorPropertyName
    val rocParams = RocParams(arguments.rocThreshold.getOrElse(List(0, 0.05, 1)))

    // Specifying variable type due to Scala bug
    val enableRoc: Boolean = arguments.enableRoc.getOrElse(false)
    val propertyType: Option[String] = arguments.propertyType
    val vertexTypeKey: Option[String] = arguments.vertexTypeKey
    val splitTypes: Option[List[String]] = arguments.splitTypes
    val numBuckets: Int = arguments.numBuckets.getOrElse(30)

    // Create graph connection
    val titanConfiguration = new SerializableBaseConfiguration()
    val titanLoadConfig = config.getConfig("titan.load")
    for (entry <- titanLoadConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanLoadConfig.getString(entry.getKey))
    }

    titanConfiguration.setProperty("storage.tablename", "iat_graph_" + graph.name) // "iat_graph_mygraph") graph.name)

    val titanConnector = new TitanGraphConnector(titanConfiguration)
    val sc = invocation.sparkContext
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    val graphElementRDD = propertyType match {
      case Some("VERTEX_PROPERTY") => titanReaderRDD.filterVertices()
      case Some("EDGE_PROPERTY") => titanReaderRDD.filterEdges()
      case _ => throw new IllegalArgumentException("Input Property Type is not supported!")
    }

    // Parse features
    val featureVectorRDD = graphElementRDD.map(element => {
      FeatureVector.parseGraphElement(element, priorPropertyName, posteriorPropertyName, vertexTypeKey)
    })

    // Filter by split type
    val filteredFeatureRDD = if (splitTypes != None) {
      featureVectorRDD.filter(f => splitTypes.get.contains(f.splitType))
    }
    else featureVectorRDD
    filteredFeatureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // Compute histograms
    val priorHistograms = FeatureVector.getHistograms(filteredFeatureRDD, false, numBuckets)
    val posteriorHistograms = if (posteriorPropertyName != None) {
      Some(FeatureVector.getHistograms(filteredFeatureRDD, true, numBuckets))
    }
    else None

    // Compute ROC curves for each feature and split type
    val rocCurves = if (enableRoc) {
      Some(FeatureVector.getRocCurves(filteredFeatureRDD, rocParams))
    }
    else None

    filteredFeatureRDD.unpersist()

    HistogramRocResult(priorHistograms, posteriorHistograms, rocCurves)

  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[HistogramRocParams]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: HistogramRocResult): JsObject = returnValue.toJson.asJsObject

  def serializeArguments(arguments: HistogramRocParams): JsObject = arguments.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/query/histogram_roc"
}