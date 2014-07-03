package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import scala.Some
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import spray.json.DefaultJsonProtocol._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils
import java.util.Date
import org.apache.spark.storage.StorageLevel

/**
 * Get histogram and optionally ROC curve on property values
 *
 * @param graph The graph ID
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
case class HistogramRocParams(graph: Int,
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
                              rocCurves: Option[List[List[(Option[String], List[Roc])]]])


/*class HistogramRocQuery extends SparkCommandPlugin[HistogramRocParams, HistogramRocResult] {
  implicit val histogramRocParamsFormat = jsonFormat9(HistogramRocParams)
  implicit val rocFormat = jsonFormat3(Roc)
  implicit val rocResultFormat = jsonFormat(RocResult, "featureId", "splitType", "rocList")
  implicit val histogramRocResultFormat = jsonFormat(HistogramRocResult, "roc")

  override def execute(invocation: SparkInvocation, arguments: HistogramRocParams)(implicit user: UserPrincipal, executionContext: ExecutionContext): HistogramRocResult = {
    //val graphFuture = invocation.engine.getGraph(arguments.graph.toLong) */
object HistogramRocQuery {
  def main(args: Array[String]) {
    // Properties
    val queryArgs = HistogramRocParams(1, "lbp_prior", Some("lbp_posterior"), enableRoc = Some(true))
    val priorPropertyName =  queryArgs.priorPropertyName
    val posteriorPropertyName = queryArgs.posteriorPropertyName
    val enableRoc : Boolean = queryArgs.enableRoc.get
    val rocParams  = RocParams(queryArgs.rocThreshold.get)
    val propertyType : Option[String] = queryArgs.propertyType
    val vertexTypeKey : Option[String] = queryArgs.vertexTypeKey
    val splitTypes : List[String] = queryArgs.splitTypes.get
    val numBuckets : Int = queryArgs.numBuckets.get

    // Create graph connection
    val tableName = "lbp_test"
    val hBaseZookeeperQuorum = "10.10.68.157"

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")
    titanConfig.setProperty("storage.hostname", hBaseZookeeperQuorum)
    titanConfig.setProperty("storage.tablename", tableName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read graph
    val conf = new SparkConf()
      //.setMaster("spark://gao-ws9.hf.intel.com:7077")
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "32")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.cores.max", "10")

    val sc = new SparkContext(conf)

    //val sc = invocation.sparkContext
    //sc.addJar("/home/spkavuly/git/source_code/engine-spark/target/engine-spark.jar")
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    val graphElementRDD = queryArgs.propertyType match {
      case Some("VERTEX_PROPERTY") => titanReaderRDD.filterVertices()
      case Some("EDGE_PROPERTY") => titanReaderRDD.filterEdges()
      case _ => throw new IllegalArgumentException("Input Property Type is not supported!")
    }

    // Parse features
    val featureVectorRDD = graphElementRDD.flatMap(element => {
      FeatureVector.parseGraphElement(element, priorPropertyName, posteriorPropertyName, vertexTypeKey)
    })
    featureVectorRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // Compute histograms
    val priorHistograms = FeatureVector.getHistograms(featureVectorRDD, false, numBuckets)

    val posteriorHistograms = if (posteriorPropertyName != None) {
      Some(FeatureVector.getHistograms(featureVectorRDD, true, numBuckets))
    }
    else None

    // Compute ROC curves for each feature and split type
    val rocCurves = if (enableRoc) {
      Some(FeatureVector.getRocCurves(featureVectorRDD, rocParams))
    }
    else None

    featureVectorRDD.unpersist()

    HistogramRocResult(priorHistograms, posteriorHistograms, rocCurves)

  }

  //TODO: Replace with generic code that works on any case class
  //def parseArguments(arguments: JsObject) = arguments.convertTo[HistogramRocParams]

  //TODO: Replace with generic code that works on any case class
  //def serializeReturn(returnValue: HistogramRocResult): JsObject = returnValue.toJson.asJsObject

  //def serializeArguments(arguments: HistogramRocParams): JsObject = arguments.toJson.asJsObject()

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  //override def name: String = "graphs/query/histogram_roc"
}