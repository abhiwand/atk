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

import com.intel.giraph.algorithms.lp.LabelPropagationComputation
import com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4LP
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LP
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.{ Config, ConfigObject, ConfigValue }
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.GiraphJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import scala.collection.JavaConverters._

case class Lp(graph: GraphReference,
              vertex_value_property_list: Option[String],
              edge_value_property_list: Option[String],
              input_edge_label_list: Option[String],
              output_vertex_property_list: Option[String],
              vector_value: Option[String],
              max_supersteps: Option[Int] = None,
              convergence_threshold: Option[Double] = None,
              anchor_threshold: Option[Double] = None,
              lp_lambda: Option[Double] = None,
              bidirectional_check: Option[Boolean] = None)
case class LpResult(runTimeSeconds: Double) //TODO

class LabelPropagation
    extends CommandPlugin[Lp, LpResult] {
  import DomainJsonProtocol._
  implicit val lbpFormat = jsonFormat11(Lp)
  implicit val lbpResultFormat = jsonFormat1(LpResult)

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

  override def execute(invocation: Invocation, arguments: Lp)(implicit user: UserPrincipal, executionContext: ExecutionContext): LpResult = {
    val start = System.currentTimeMillis()

    System.out.println("Starting LabelPropagation execution")

    val config = configuration().get
    val hConf = newHadoopConfigurationFrom(config, "giraph")
    val titanConf = flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    set(hConf, "lp.maxSuperSteps", arguments.max_supersteps)
    set(hConf, "lp.convergenceThreshold", arguments.convergence_threshold)
    set(hConf, "lp.anchorThreshold", arguments.anchor_threshold)
    set(hConf, "lp.bidirectionalCheck", arguments.bidirectional_check)

    set(hConf, "giraph.titan.input.storage.backend", titanConf.get("titan.load.storage.backend"))
    set(hConf, "giraph.titan.input.storage.hostname", titanConf.get("titan.load.storage.hostname"))
    // TODO - graph should provide backend to retrieve the stored table name in hbase
    set(hConf, "giraph.titan.input.storage.tablename", Option[Any]("iat_graph_" + graph.name))
    set(hConf, "giraph.titan.input.storage.port", titanConf.get("titan.load.storage.port"))

    set(hConf, "input.vertex.value.property.key.list", arguments.vertex_value_property_list)
    set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    set(hConf, "vector.value", arguments.vector_value)

    val giraphLoader = Boot.getClassLoader(config.getString("giraph.archive.name"))
    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4LP])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LP[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.VertexData4LPWritable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[LabelPropagationComputation.LabelPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LabelPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LabelPropagationComputation.LabelPropagationAggregatorWriter])

    Thread.currentThread().setContextClassLoader(giraphLoader)

    val job = new GiraphJob(giraphConf, "iat-giraph-lp")
    val internalJob: Job = job.getInternalJob
    val algorithm = giraphLoader.loadClass(classOf[LabelPropagationComputation].getCanonicalName)
    internalJob.setJarByClass(algorithm)

    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      new Path("/user/hadoop/lp/out/" + invocation.commandId))

    job.run(true)

    val time = (System.currentTimeMillis() - start).toDouble / 1000.0
    LpResult(time)
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Lp]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: LpResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/ml/label_propagation"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Lp): JsObject = arguments.toJson.asJsObject()
}
