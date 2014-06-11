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

package com.intel.intelanalytics.engine.spark.algorithm

import com.intel.intelanalytics.domain.DomainJsonProtocol
import scala.concurrent._
import scala.Some
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.engine.Execution
import spray.json._
import DomainJsonProtocol._
import scala.concurrent.duration._
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.{GiraphConfigurationValidator, GiraphJob}
import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation
import com.intel.giraph.io.formats.{JsonPropertyGraph4LBPOutputFormat, JsonPropertyGraph4LBPInputFormat}
import org.apache.hadoop.fs.Path
import com.intel.intelanalytics.engine.hadoop.HadoopSupport
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.giraph.GiraphRunner
import com.intel.intelanalytics.component.Boot
import java.util.Date
import org.apache.hadoop.io.{WritableComparable, Writable}
import com.intel.intelanalytics.engine.hadoop.HadoopSupport

object LoopyBeliefPropagation
  extends Execution.CommandDefinition
  with HadoopSupport {


  case class Lbp[GraphRef](graph: GraphRef,
                           max_supersteps: Option[Int] = None,
                           convergence_threshold: Option[Double] = None,
                           anchor_threshold: Option[Double] = None,
                           smoothing: Option[Double] = None,
                           bidirectional_check: Option[Boolean] = None,
                           ignore_vertex_type: Option[Boolean] = None,
                           max_product: Option[Boolean] = None,
                           power: Option[Double] = None)

  implicit val lbpFormat = jsonFormat9(Lbp[Long])

  override def execute(execution: Execution.CommandExecution): JsObject = withContext("lbp.apply") {

    val lbp = execution.arguments.getOrElse(
      illegalArg("Arguments required for loopy belief propagation"))
      .convertTo[Lbp[Long]]
    implicit val execCtx = execution.executionContext
    val graphFuture = execution.engine.getGraph(lbp.graph)

    val hConf = newHadoopConfigurationFrom(execution.config, "giraph")



    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    set(hConf, "lbp.maxSuperSteps", lbp.max_supersteps)
    set(hConf, "lbp.convergenceThreshold", lbp.convergence_threshold)
    set(hConf, "lbp.anchorThreshold", lbp.anchor_threshold)
    set(hConf, "lbp.bidirectionalCheck", lbp.bidirectional_check)
    set(hConf, "lbp.power", lbp.power)
    set(hConf, "lbp.smoothing", lbp.smoothing)
    set(hConf, "lbp.ignoreVertexType", lbp.ignore_vertex_type)

    val graph = Await.result(graphFuture, execution.config.getInt("default-timeout") seconds)

    //val yarnConfig: YarnConfiguration = new YarnConfiguration(hConf)
    val giraphConf = new GiraphConfiguration(hConf)
    //TODO: replace these with titan formats when we have titan working.
    giraphConf.setVertexInputFormatClass(classOf[JsonPropertyGraph4LBPInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[JsonPropertyGraph4LBPOutputFormat])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationAggregatorWriter])
    val graphId = lbp.graph //graph.getOrElse(illegalArg("Graph does not exist, cannot run LBP")
    //val job = new org.apache.giraph.yarn.GiraphYarnClient(giraphConf, "iat-giraph-lbp-" + new Date());
    val job = new GiraphJob(giraphConf, "iat-giraph-lbp-" + new Date())
    val internalJob: Job = job.getInternalJob
    val giraphLoader = Boot.getClassLoader(execution.config.getString("giraph.archive.name"))
    Thread.currentThread().setContextClassLoader(giraphLoader)
    val coreSiteXml = giraphLoader.getResource("core-site.xml")
    assert(coreSiteXml != null, "core-site.xml not available on class path")
    println("Found: " + coreSiteXml.getFile)
    val algorithm = giraphLoader.loadClass(classOf[LoopyBeliefPropagationComputation].getCanonicalName)
    internalJob.setJarByClass(algorithm)
    //internalJob.setJar(execution.config.getString("giraph-jar"))
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(internalJob,
      new Path("/user/hadoop/lbp/in"))
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(internalJob,
      new Path("/user/hadoop/lbp/out/" + execution.commandId))
    @SuppressWarnings(Array("rawtypes")) val gtv: GiraphConfigurationValidator[_, _, _, _, _] =
      new GiraphConfigurationValidator(giraphConf)


    gtv.validateConfiguration()
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
    JsObject()
  }
}
