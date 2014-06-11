package com.intel.intelanalytics.engine.spark.context

import com.typesafe.config.Config

/**
 * Base class for different Spark context management strategies
 */
trait SparkContextManagementStrategy {
  var configuration: Config = null
  var sparkContextFactory: SparkContextFactory = null

  def getContext(user: String): Context
  def cleanup(): Unit
  def removeContext(user: String): Unit
  def getAllContexts(): List[Context]
}
