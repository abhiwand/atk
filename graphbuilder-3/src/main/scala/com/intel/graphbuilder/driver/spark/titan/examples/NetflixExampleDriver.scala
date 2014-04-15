package com.intel.graphbuilder.driver.spark.titan.examples

import com.intel.graphbuilder.driver.spark.titan.{GraphBuilder, GraphBuilderConfig}
import com.intel.graphbuilder.parser.ColumnDef
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule.{EdgeRule, VertexRule}
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import java.util.Date
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Example of building a Graph using a Netflix file as input.
 */
object NetflixExampleDriver {

  // Spark Settings
  val master = "spark://GAO-WSE.jf.intel.com:7077"
  //val master = "local"
  val sparkHome = "/opt/cloudera/parcels/CDH/lib/spark/"
  val gbJar = "/home/hadoop/source_code/graphbuilder-3/target/scala-2.10/gb.jar"

  // Titan Settings
  val titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.backend", "cassandra")
  titanConfig.setProperty("storage.hostname", "127.0.0.1")
  titanConfig.setProperty("storage.keyspace", "netflix")
  titanConfig.setProperty("storage.batch-loading", "true")
  titanConfig.setProperty("autotype", "none")
  //titanConfig.setProperty("storage.buffer-size", "4096")
  titanConfig.setProperty("storage.attempt-wait", "300")
  titanConfig.setProperty("storage.lock-wait-time", "400")
  titanConfig.setProperty("storage.lock-retries", "15")
  titanConfig.setProperty("ids.block-size", "300000")
  titanConfig.setProperty("ids.renew-timeout", "150000")

  val inputPath = "hdfs://GAO-WSE.jf.intel.com/data/movie_data_1mb.csv"

  // Input Schema
  val inputSchema = new InputSchema(List(
    new ColumnDef("userId", classOf[String]),
    new ColumnDef("vertexType", classOf[String]),
    new ColumnDef("movieId", classOf[String]),
    new ColumnDef("rating", classOf[String]),
    new ColumnDef("splits", classOf[String])
  ))

  // Parser Configuration
  val vertexRules = List(VertexRule(property("userId"), List(property("vertexType"))), VertexRule(property("movieId")))
  val edgeRules = List(EdgeRule(property("userId"), property("movieId"), "rates", List(property("rating"), property("splits"))))

  /**
   * This is an example of building a graph on Titan with a Cassandra backend using Spark.
   */
  def main(args: Array[String]) {

    println("start " + new Date())

    // Initialize Spark Connection
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(this.getClass.getSimpleName + " " + new Date())
      .setSparkHome(sparkHome)
      .setJars(List(gbJar))
    conf.set("spark.executor.memory", "32g")
    conf.set("spark.cores.max", "16")

    val sc = new SparkContext(conf)

    // Setup data in Spark
    val inputRows = sc.textFile(inputPath, 240) /* 240 worked for 1gb file, smaller values like 90 were failing with OutOfMemory for 1GB */
    val inputRdd = inputRows.map(row => row.split(","): Seq[_])

    // Build the Graph
    val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, biDirectional = false, append = false)
    val gb = new GraphBuilder(config)
    gb.build(inputRdd)

    println("done " + new Date())

  }

}
