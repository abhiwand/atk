package com.intel.intelanalytics.engine.spark.context

import org.apache.spark.SparkConf
import com.typesafe.config.Config
import org.apache.spark.engine.ProgressPrinter
import com.intel.intelanalytics.engine.spark.context.{SparkContextManagementStrategy, SparkContextFactory, Context}



class SparkContextManager(conf: Config, factory: SparkContextFactory) extends SparkContextManagementStrategy {
  //TODO read the strategy from the config file
  val contextManagementStrategy: SparkContextManagementStrategy = SparkContextPerUserStrategy
  contextManagementStrategy.configuration = conf
  contextManagementStrategy.sparkContextFactory = factory

  def getContext(user: String): Context = { contextManagementStrategy.getContext(user) }
  def cleanup(): Unit = { contextManagementStrategy.cleanup() }
  def removeContext(user: String): Unit = { contextManagementStrategy.removeContext(user) }
  def getAllContexts(): List[Context] = { contextManagementStrategy.getAllContexts() }
}