package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{ Edge, Vertex }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.storage.StorageLevel
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent._
import com.intel.intelanalytics.domain.command.CommandDoc

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
 * @param enable_roc True means to plot ROC curve on the validation (VA) and test(TE) splits of
 *                   the prior and posterior values, as well as calculate the AUC value on each
 *                   feature dimension of the prior and posterior values.
 *                   False means not to plot ROC curve.
 *                   The default value is False.
 * @param roc_threshold The ROC threshold parameters in "min:step:max" format.
 *                      The default value is "0:0.05:1"
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
case class HistogramRocParams(graph: GraphReference,
                              prior_property_list: String,
                              posterior_property_list: Option[String],
                              enable_roc: Option[Boolean],
                              roc_threshold: Option[List[Double]],
                              property_type: Option[String],
                              vertex_type_property_key: Option[String],
                              split_types: Option[List[String]],
                              histogram_buckets: Option[Int]) {
  require(roc_threshold == None || roc_threshold.get.size == 3, "Please input roc_threshold using [min, step, max] format")
}

/**
 * Algorithm report comprising of histogram of prior and posterior probabilities, and ROC curves.
 *
 * @param prior_histograms Histograms of prior probabilities (one for each feature)
 * @param posterior_histograms Histograms of posterior probabilities (one for each feature)
 * @param roc_curves ROC curves for each feature and split type
 */
case class HistogramRocResult(prior_histograms: List[Histogram],
                              posterior_histograms: Option[List[Histogram]],
                              roc_curves: Option[List[List[RocCurve]]])

class HistogramRocQuery extends SparkCommandPlugin[HistogramRocParams, HistogramRocResult] {

  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val histogramRocParamsFormat = jsonFormat9(HistogramRocParams)
  implicit val rocFormat = jsonFormat3(Roc)
  implicit val rocCurveFormat = jsonFormat2(RocCurve)
  implicit val histogramFormat = jsonFormat2(Histogram.apply)
  implicit val histogramRocResultFormat = jsonFormat3(HistogramRocResult)

  override def doc = Some(CommandDoc(oneLineSummary = "Generate histograms, and Receiver Operating Characteristic (ROC) curves.",
    extendedSummary = Some("""
    Extended Summary
    ----------------
    Generate histograms of prior and posterior probabilities, and Receiver Operating Characteristic (ROC)
    curves for the model.

    The prerequisite is that either LBP, ALS or CGD has been run before this query. Prior histograms are
    available for LBP, ALS and CGD. ROC curves are only available for LBP because ROC curves require
    both prior and posterior probabilities to be present.

    Parameters
    ----------
    prior_property_list : String
        Name of the property containing the vector of prior probabilities.
        The prior probabilities are represented in the graph as a delimited list
        of real values between [0,1], one for each feature dimension.

    posterior_property_list: String (optional)
        Name of the property containing the vector of posterior probabilities.
        The posterior probabilities are represented in the graph as a delimited list
        of real values between [0,1], one for each feature dimension.

    enable_roc : Boolean (optional)
        If True, generates ROC curves for the train (TR), validation (VA) and test (TE) splits of
        each feature dimension. The ROC curves are computed using the prior and posterior probabilities.
        False disables the generation of ROC curves.
        The default value is False.

    roc_threshold : List of Doubles (optional)
        The ROC threshold parameters in [min, step, max] format.
        The default value is [0, 0.05, 1].

  property_type : String (optional)
        The type of property for the prior and posterior values.
        Valid values are either VERTEX_PROPERTY or EDGE_PROPERTY.
        The default value is VERTEX_PROPERTY.

    vertex_type_property_key : String (optional)
        The property name for vertex type. The default value "vertex_type".
        This property indicates whether the data is in the train, validation, or test splits.

    split_types : List of Strings (optional)
        The list of split types to include in the report.
        The default value is =["TR", "VA", "TE"] for train (TR), validation (VA), and test (TE) splits.

    histogram_buckets : int32
        The number of buckets to plot in histograms. The default value is 30.

    Raises
    ------
    RuntimeException
        If the properties specified do not exist in the graph, or if the dimensions of the prior and posterior
        vectors are not the same.

    Returns
    -------
        Dictionary containing prior histograms, and optionally the posterior histograms and ROC curves. The dictionary
        entries are:
          prior_histograms : An array of histograms of prior probabilities for each feature dimension.
            The histogram comprises of an array of buckets and corresponding counts. The buckets are all open
            to the left except for the last which is closed, e.g., for the array [1,5,10] the buckets are
            [1, 5) [5, 10]. The size of the counts array is smaller than the buckets array by 1.

        posterior_histograms : An array of histograms of posterior probabilities for each feature dimension.

        roc_curves: An array of arrays containing the ROC curves for each feature dimension and split type.

    Examples
    --------
    For example, you can generate the prior and posterior histograms, and ROC curves for LBP as follows:

      graph = BigGraph(...)
      graph.ml.loopy_belief_propagation(...)
      results = graph.query.histogram_roc(prior_property_list ="value", posterior_property_list = "lbp_posterior", enable_roc = True, roc_threshold =[0, 0.05, 1], property_type = "VERTEX_PROPERTY", vertex_type_property_key="vertex_type",  split_types=["TR", "VA", "TE"], histogram_buckets=30)

      results["prior_histograms"]
      results["posterior_histograms"]
      results["roc_curves"]

    If you want compute only the prior histograms use:
      results = graph.query.histogram_roc(prior_property_list ="value", enable_roc = False)
""")))

  override def execute(invocation: SparkInvocation, arguments: HistogramRocParams)(implicit user: UserPrincipal, executionContext: ExecutionContext): HistogramRocResult = {
    import scala.concurrent.duration._

    System.out.println("*********In Execute method of Histogram ROC query********")
    val config = configuration
    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    val defaultRocParams = config.getDoubleList("roc-threshold").asScala.toList.map(_.doubleValue())
    val rocParams = RocParams(arguments.roc_threshold.getOrElse(defaultRocParams))

    // Specifying variable type due to Scala bug
    val enableRoc = arguments.enable_roc.getOrElse(config.getBoolean("enable-roc"))
    val numBuckets = arguments.histogram_buckets.getOrElse(config.getInt("histogram-buckets"))
    val propertyClass = arguments.property_type match {
      case Some("EDGE_PROPERTY") => classOf[Edge]
      case Some("VERTEX_PROPERTY") => classOf[Vertex]
      case None => classOf[Vertex]
      case _ => throw new IllegalArgumentException("Please input 'VERTEX_PROPERTY' or 'EDGE_PROPERTY' for property_type")
    }

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
    val graphElementRDD = if (propertyClass.isInstanceOf[Edge]) {
      titanReaderRDD.filterEdges()
    }
    else titanReaderRDD.filterVertices()

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
    val priorHistograms = FeatureVector.getHistograms(filteredFeatureRDD, false, numBuckets)
    val posteriorHistograms = if (arguments.posterior_property_list != None) {
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