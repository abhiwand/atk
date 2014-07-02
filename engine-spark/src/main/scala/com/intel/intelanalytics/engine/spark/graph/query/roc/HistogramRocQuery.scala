package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import org.apache.spark.SparkContext._
import scala.Some
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import spray.json._
import spray.json.DefaultJsonProtocol._

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
 * @param binNum The bin number to plot histogram. The default value is 30.
 */
case class HistogramRocParams(graph: Int,
                              priorPropertyName: String,
                              posteriorPropertyName: Option[String] = Some(""),
                              enableRoc: Option[Boolean] = Some(false),
                              rocThreshold: Option[List[Double]] = Some(List(0, 0.05, 1)),
                              propertyType: Option[String] = Some("VERTEX_PROPERTY"),
                              vertexTypeKey: Option[String] = Some("vertex_type"),
                              splitTypes: Option[List[String]] = Some(List("TR", "VA", "TE")),
                              binNum: Option[Int] = Some(30)) {
  require(rocThreshold.get.size == 3)
}

/**
 * Returns
 * -------
 * output : AlgorithmReport
 * Execution time, and AUC values on each feature if ROC is enabled.
 */
case class HistogramRocResult(roc: List[RocResult]) extends Serializable

case class RocResult(featureId: Long, splitType: Option[String], rocList: List[Roc]) extends Serializable

class HistogramRocQuery extends SparkCommandPlugin[HistogramRocParams, HistogramRocResult] {
  implicit val histogramRocParamsFormat = jsonFormat9(HistogramRocParams)
  implicit val rocFormat = jsonFormat3(Roc)
  implicit val rocResultFormat = jsonFormat(RocResult, "featureId", "splitType", "rocList")
  implicit val histogramRocResultFormat = jsonFormat(HistogramRocResult, "roc")

  override def execute(invocation: SparkInvocation, arguments: HistogramRocParams)(implicit user: UserPrincipal, executionContext: ExecutionContext): HistogramRocResult = {
    //val graphFuture = invocation.engine.getGraph(arguments.graph.toLong)
    //def main(args: Array[String]) {
    // Properties
    val rocArgs = HistogramRocParams(1, "lbp_prior", Some("lbp_posterior"), enableRoc = Some(true))
    val rocParams = RocParams(rocArgs.rocThreshold.get)

    // Create graph connection
    val tableName = "lbp_test"
    val hBaseZookeeperQuorum = "10.10.68.157"

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")
    titanConfig.setProperty("storage.hostname", hBaseZookeeperQuorum)
    titanConfig.setProperty("storage.tablename", tableName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read graph
    /*val conf = new SparkConf()
      .setMaster("spark://gao-ws9.hf.intel.com:7077")
      //.setMaster("local")
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(ExamplesUtils.sparkHome)
      .setJars(List(ExamplesUtils.gbJar))

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.intel.graphbuilder.driver.spark.titan.GraphBuilderKryoRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "32")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.cores.max", "10")

    val sc = new SparkContext(conf)  */

    val sc = invocation.sparkContext
    //sc.addJar("/home/spkavuly/git/source_code/engine-spark/target/engine-spark.jar")
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    val graphElementRDD = rocArgs.propertyType match {
      case Some("VERTEX_PROPERTY") => titanReaderRDD.filterVertices()
      case Some("EDGE_PROPERTY") => titanReaderRDD.filterEdges()
      case _ => throw new IllegalArgumentException("Input Property Type is not supported!")
    }

    val featureRDD = graphElementRDD.flatMap(element => {
      FeatureProbability.parseGraphElement(element, rocArgs.priorPropertyName, rocArgs.posteriorPropertyName, rocArgs.vertexTypeKey)
    })

    // Get feature histogram
    val priorProbabilities = featureRDD.map(f => (f.featureId, f.priorProbability)).groupBy(_._1)
    //val priorHistogram = priorProbabilities.map(pair => pair._2).histogram(rocArgs.binNum.get)

    /*val rocCountRDD = featureRDD.map(feature => ((feature.featureId, feature.splitType), FeatureProbability.calcRoc(feature, rocParams))).reduceByKey((a1, a2) => a1.add(a2))
    println("rocCountRDD:" + rocCountRDD.first())

    val updatedRocRdd = rocCountRDD.map(rocCounts => RocResult(rocCounts._1._1, rocCounts._1._2, FeatureProbability.updateRoc(rocCounts._2, rocParams)))
    println("updatedRocRdd:" + updatedRocRdd.first())

    val roc = updatedRocRdd.collect().toList
    println("roc:" + roc(0))                 */
    HistogramRocResult(roc = List(RocResult(featureRDD.count(), Some("test"), List(Roc(1, 0, 0)))))

  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[HistogramRocParams]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: HistogramRocResult): JsObject = returnValue.toJson.asJsObject

  def serializeArguments(arguments: HistogramRocParams): JsObject = arguments.toJson.asJsObject()

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/query/histogram_roc"
}