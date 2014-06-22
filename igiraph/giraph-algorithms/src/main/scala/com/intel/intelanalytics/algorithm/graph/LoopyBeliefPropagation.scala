//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.algorithm.graph

import java.util.Date

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation
import com.intel.giraph.io.formats.{JsonPropertyGraph4LBPInputFormat, JsonPropertyGraph4LBPOutputFormat}
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.plugin.{CommandPlugin, Invocation}
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.{Config, ConfigObject, ConfigValue}
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.io.formats.GiraphFileInputFormat
import org.apache.giraph.job.GiraphJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent._

case class Lbp(graph: Int,
               max_supersteps: Option[Int] = None,
               convergence_threshold: Option[Double] = None,
               anchor_threshold: Option[Double] = None,
               smoothing: Option[Double] = None,
               bidirectional_check: Option[Boolean] = None,
               ignore_vertex_type: Option[Boolean] = None,
               max_product: Option[Boolean] = None,
               power: Option[Double] = None)

case class LbpResult(runTimeSeconds: Double) //TODO

class LoopyBeliefPropagation
  extends CommandPlugin[Lbp, LbpResult] {
  implicit val lbpFormat = jsonFormat9(Lbp)
  implicit val lbpResultFormat = jsonFormat1(LbpResult)


  /**
   * Set a value in the hadoop configuration, if the argument is not None.
   * @param hadoopConfiguration the configuration to update
   * @param hadoopKey the key name to set
   * @param arg the value to use, if it is defined
   */
  def set(hadoopConfiguration: Configuration, hadoopKey: String, arg: Option[Any]) = arg.foreach { value =>
    hadoopConfiguration.set(hadoopKey, value.toString)
  }

  /**
   * Create new Hadoop Configuration object, preloaded with the properties
   * specified in the given Config object under the provided key.
   * @param config the Config object from which to copy properties to the Hadoop Configuration
   * @param key the starting point in the Config object. Defaults to "hadoop".
   * @return a populated Hadoop Configuration object.
   */
  def newHadoopConfigurationFrom(config: Config, key: String = "hadoop") = {
    require(config != null, "Config cannot be null")
    require(key != null, "Key cannot be null")
    val hConf = new Configuration()
    val properties = flattenConfig(config.getConfig(key))
    properties.foreach { kv =>
      println(s"Setting ${kv._1} to ${kv._2}")
      hConf.set(kv._1, kv._2) }
    hConf
  }

  /**
   * Flatten a nested Config structure down to a simple dictionary that maps complex keys to
   * a string value, similar to java.util.Properties.
   *
   * @param config the config to flatten
   * @return a map of property names to values
   */
  private def flattenConfig(config: Config, prefix: String = "") : Map[String, String] = {
    val result = config.root.asScala.foldLeft(Map.empty[String,String]) {
      (map, kv) => kv._2 match {
        case co: ConfigObject =>
          val nested = flattenConfig(co.toConfig, prefix = prefix + kv._1 + ".")
          map ++ nested
        case value: ConfigValue =>
          map + (prefix + kv._1 -> value.unwrapped().toString)
      }
    }
    result
  }

  override def execute(invocation: Invocation, arguments: Lbp)
                      (implicit user: UserPrincipal, executionContext:ExecutionContext): LbpResult =  {
    val start = System.currentTimeMillis()
//    val graphFuture = invocation.engine.getGraph(arguments.graph.toLong)

    val config: Config = configuration().get
    val hConf = newHadoopConfigurationFrom(config, "giraph")




    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    set(hConf, "lbp.maxSuperSteps", arguments.max_supersteps)
    set(hConf, "lbp.convergenceThreshold", arguments.convergence_threshold)
    set(hConf, "lbp.anchorThreshold", arguments.anchor_threshold)
    set(hConf, "lbp.bidirectionalCheck", arguments.bidirectional_check)
    set(hConf, "lbp.power", arguments.power)
    set(hConf, "lbp.smoothing", arguments.smoothing)
    set(hConf, "lbp.ignoreVertexType", arguments.ignore_vertex_type)

//    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //val yarnConfig: YarnConfiguration = new YarnConfiguration(hConf)
    val giraphConf = new GiraphConfiguration(hConf)
    //TODO: replace these with titan formats when we have titan working.
    giraphConf.setVertexInputFormatClass(classOf[JsonPropertyGraph4LBPInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[JsonPropertyGraph4LBPOutputFormat])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationAggregatorWriter])

    GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path("/user/hadoop/lbp/in"))


    val graphId = arguments.graph //graph.getOrElse(illegalArg("Graph does not exist, cannot run LBP")
    //val job = new org.apache.giraph.yarn.GiraphYarnClient(giraphConf, "iat-giraph-lbp-" + new Date());
    val job = new GiraphJob(giraphConf, "iat-giraph-lbp-" + new Date())
    val internalJob: Job = job.getInternalJob
    val giraphLoader = Boot.getClassLoader(config.getString("giraph.archive.name"))
    Thread.currentThread().setContextClassLoader(giraphLoader)
    val coreSiteXml = giraphLoader.getResource("core-site.xml")
    assert(coreSiteXml != null, "core-site.xml not available on class path")
    println("Found: " + coreSiteXml.getFile)
    val algorithm = giraphLoader.loadClass(classOf[LoopyBeliefPropagationComputation].getCanonicalName)
    internalJob.setJarByClass(algorithm)
    //internalJob.setJar(execution.config.getString("giraph-jar"))

    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(internalJob,
      new Path("/user/hadoop/lbp/in"))
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      new Path("/user/hadoop/lbp/out/" + invocation.commandId))
 
//   @SuppressWarnings(Array("rawtypes")) val gtv: GiraphConfigurationValidator[_, _, _, _, _] =
//      new GiraphConfigurationValidator(giraphConf)
//
//
//    gtv.validateConfiguration()
    job.run(true)
    //    val runner = giraphLoader.loadClass(classOf[GiraphRunner].getCanonicalName)
    //    val method = runner.getMethod("main", classOf[Array[String]])
    //    method.invoke(null, Array("com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation",
    //      "-vif", "com.intel.giraph.io.formats.JsonPropertyGraph4LBPInputFormat",
    //      "-vip", "input/synth.pg.json_training",
    //      "-vof", " com.intel.giraph.io.formats.JsonPropertyGraph4LBPOutputFormat",
    //      "-op", "output/lbp_test",
    //      "-mc", "com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation$LoopyBeliefPropagationMasterCompute",
    //      "-aw", "com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation$LoopyBeliefPropagationAggregatorWriter",
    //      "-w", "1",
    //      "-ca", "giraph.SplitMasterWorker=true",
    //      "-ca", "lbp.maxSupersteps=10",
    //      "-ca", "lbp.smoothing=2",
    //      "-ca", "lbp.convergenceThreshold=0",
    //      "-ca", "lbp.anchorThreshold=0.9",
    //      "-ca", "lbp.ignoreVertexType=true",
    //      "-ca", "lbp.maxProduct=false",
    //      "-ca", "lbp.power=0.5"))
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0
    LbpResult(time)
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Lbp]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: LbpResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/ml/loopy_belief_propagation"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Lbp): JsObject = arguments.toJson.asJsObject()
}
