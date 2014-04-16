package com.intel.graphbuilder.testutils

import org.apache.log4j.{Logger, Level}

/**
 * Utility methods related to logging in Unit testing.
 * <p>
 * Logging of underlying libraries can get annoying in unit
 * tests so it is nice to be able to change easily.
 * </p>
 */
object LogUtils {

  /**
   * Turn down logging since Spark gives so much output otherwise.
   */
  def silenceSpark() {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  /**
   * Turn down logging for Titan
   */
  def silenceTitan() {
    setLogLevels(Level.WARN, Seq("com.thinkaurelius"))
    setLogLevels(Level.ERROR, Seq("com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx"))
  }

  private def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]): Unit = {
    loggers.foreach(loggerName => Logger.getLogger(loggerName).setLevel(level))
  }

}
