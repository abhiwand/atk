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
import com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4LBP
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LBP
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.{ Config, ConfigObject, ConfigValue }
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.GiraphJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._
import java.net.URI

import scala.concurrent._
import scala.collection.JavaConverters._

case class Lbp(graph: GraphReference,
               vertex_value_property_list: Option[String],
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               convergence_threshold: Option[Double] = None,
               anchor_threshold: Option[Double] = None,
               smoothing: Option[Double] = None,
               bidirectional_check: Option[Boolean] = None,
               ignore_vertex_type: Option[Boolean] = None,
               max_product: Option[Boolean] = None,
               power: Option[Double] = None)

case class LbpResult(value: String) //TODO

class LoopyBeliefPropagation
    extends CommandPlugin[Lbp, LbpResult] {
  import DomainJsonProtocol._
  implicit val lbpFormat = jsonFormat15(Lbp)
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
      hConf.set(kv._1, kv._2)
    }
    hConf
  }

  /**
   * Flatten a nested Config structure down to a simple dictionary that maps complex keys to
   * a string value, similar to java.util.Properties.
   *
   * @param config the config to flatten
   * @return a map of property names to values
   */
  private def flattenConfig(config: Config, prefix: String = ""): Map[String, String] = {
    val result = config.root.asScala.foldLeft(Map.empty[String, String]) {
      (map, kv) =>
        kv._2 match {
          case co: ConfigObject =>
            val nested = flattenConfig(co.toConfig, prefix = prefix + kv._1 + ".")
            map ++ nested
          case value: ConfigValue =>
            map + (prefix + kv._1 -> value.unwrapped().toString)
        }
    }
    result
  }

  override def execute(invocation: Invocation, arguments: Lbp)(implicit user: UserPrincipal, executionContext: ExecutionContext): LbpResult = {

    val config = configuration().get
    val hConf = newHadoopConfigurationFrom(config, "giraph")
    val titanConf = flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    set(hConf, "lbp.maxSuperSteps", arguments.max_supersteps)
    set(hConf, "lbp.convergenceThreshold", arguments.convergence_threshold)
    set(hConf, "lbp.anchorThreshold", arguments.anchor_threshold)
    set(hConf, "lbp.bidirectionalCheck", arguments.bidirectional_check)
    set(hConf, "lbp.power", arguments.power)
    set(hConf, "lbp.smoothing", arguments.smoothing)
    set(hConf, "lbp.ignoreVertexType", arguments.ignore_vertex_type)

    set(hConf, "giraph.titan.input.storage.backend", titanConf.get("titan.load.storage.backend"))
    set(hConf, "giraph.titan.input.storage.hostname", titanConf.get("titan.load.storage.hostname"))
    // TODO - graph should provide backend to retrieve the stored table name in hbase
    set(hConf, "giraph.titan.input.storage.tablename", Option[Any]("iat_graph_" + graph.name))
    set(hConf, "giraph.titan.input.storage.port", titanConf.get("titan.load.storage.port"))

    set(hConf, "input.vertex.value.property.key.list", arguments.vertex_value_property_list)
    set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    set(hConf, "vertex.type.property.key", arguments.vertex_type_property_key)
    set(hConf, "vector.value", arguments.vector_value)

    val giraphLoader = Boot.getClassLoader(config.getString("giraph.archive.name"))
    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4LBP])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LBP[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationAggregatorWriter])

    Thread.currentThread().setContextClassLoader(giraphLoader)

    val job = new GiraphJob(giraphConf, "iat-giraph-lbp")
    val internalJob: Job = job.getInternalJob
    val algorithm = giraphLoader.loadClass(classOf[LoopyBeliefPropagationComputation].getCanonicalName)
    internalJob.setJarByClass(algorithm)

    val output_dir_path = config.getString("fs.root") + "/" + config.getString("output.dir") + "/" + invocation.commandId
    val output_dir = new URI(output_dir_path)
    if (config.getBoolean("output.overwrite")) {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path(output_dir), true)
    }

    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      new Path(output_dir))

    if (job.run(true)) {
      val fs = FileSystem.get(new Configuration())
      val stream = fs.open(new Path(output_dir_path + "/" + "lbp-learning-report_0"))
      def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
      val result = readLines.takeWhile(_ != null).toList.mkString("\n")
      fs.close()
      LbpResult(result)
    }
    else LbpResult("Error: No Learning Report found!!")
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
