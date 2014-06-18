package com.intel.intelanalytics.engine.spark.context

import com.typesafe.config.Config
import com.intel.intelanalytics.security.UserPrincipal

class SparkContextManager(conf: Config, factory: SparkContextFactory) extends SparkContextManagementStrategy {
  //TODO read the strategy from the config file
  val contextManagementStrategy: SparkContextManagementStrategy = SparkContextPerUserStrategy
  contextManagementStrategy.configuration = conf
  contextManagementStrategy.sparkContextFactory = factory

  def getContext(user: String): Context = contextManagementStrategy.getContext(user)
  def context(implicit user: UserPrincipal): Context = getContext(user.user.apiKey.getOrElse(
    throw new RuntimeException("User didn't have an apiKey which shouldn't be possible if they were authenticated")))
  def cleanup(): Unit = contextManagementStrategy.cleanup()
  def removeContext(user: String): Unit = contextManagementStrategy.removeContext(user)
  def getAllContexts(): List[Context] = contextManagementStrategy.getAllContexts()
}