package com.intel.intelanalytics.engine.spark

import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.mutable
import com.typesafe.config.{ ConfigFactory, Config }
import com.intel.intelanalytics.shared.EventLogging
import org.apache.spark.engine.{ TestListener, SparkProgressListener }

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

/**
 * Had to extract SparkContext creation logic from the SparkContextManagementStrategy for better testability
 */
class SparkContextFactory {
  def createSparkContext(configuration: Config, appName: String): SparkContext = {
    val sparkHome = configuration.getString("intel.analytics.spark.home")
    val sparkMaster = configuration.getString("intel.analytics.spark.master")
    val sparkConf = new SparkConf()
      .setMaster(sparkMaster)
      .setSparkHome(sparkHome)
      .setAppName(appName)
    new SparkContext(sparkConf)
  }
}

case class Context(sparkContext: SparkContext, progressMonitor: SparkProgressListener)

/**
 * This context management strategy creates a context per user if it doesn't exist, else returns the existing context
 * SparkContext is not a lightweight object, I had to increase max procs and max users limits in the OS to
 * create in the order of hundreds of SparkContetxs pre JVM
 */
object SparkContextPerUserStrategy extends SparkContextManagementStrategy with EventLogging {

  //TODO: take a look at spark.cleaner.ttl parameter, the doc says that this param is useful for long running contexts
  val contextMap = new mutable.HashMap[String, Context] with mutable.SynchronizedMap[String, Context] {}

  //TODO: how to run jobs as a particular user
  //TODO: Decide on spark context life cycle - should it be torn down after every operation,
  //or left open for some time, and reused if a request from the same user comes in?
  //Is there some way of sharing a context across two different Engine instances?

  override def getContext(user: String): Context = {
    contextMap.get(user) match {
      case Some(ctx) => ctx
      case None => {
        //we need to clean/update some properties to get rid of Spark's port binding problems
        //when creating multiple SparkContexts within the same JVM
        System.clearProperty("spark.driver.port") //need to clear this to get rid of port bind problems
        System.setProperty("spark.ui.port", String.valueOf(4041 + contextMap.size)) //need to uniquely set this to get rid of bind problems
        val context = sparkContextFactory.createSparkContext(configuration, "intel-analytics:" + user)
        val listener = new SparkProgressListener()
        val testListener = new TestListener(listener)
        context.addSparkListener(listener)
        context.addSparkListener(testListener)
        Context(context, listener)
        val ctx = Context(context, listener)
        contextMap += (user -> ctx)
        ctx
      }
    }
  }

  /**
   * stop all managed SparkContexts
   */
  override def cleanup(): Unit = {
    contextMap.keys.foreach { key =>
      contextMap(key).sparkContext.stop()
    }
  }

  /**
   * removes the SparkContext for the given user (key) if it exists
   */
  override def removeContext(user: String): Unit = {
    if (contextMap contains user) {
      contextMap(user).sparkContext.stop()
      contextMap -= user
    }
  }

  def getAllContexts(): List[Context] = {
    contextMap.values.toList
  }

}