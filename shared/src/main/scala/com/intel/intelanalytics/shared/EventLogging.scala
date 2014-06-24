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

package com.intel.intelanalytics.shared

import com.intel.event.adapter.SLF4JLogAdapter
import com.intel.event.{EventLog, EventLogger, Severity, EventContext}
import scala.util.control.NonFatal

/**
 * Mixin for logging with the Event library.
 */
trait EventLogging {
  val setEventLog: EventLog = {
    if (EventLogger.getImplementation() == null) {
      println("START shared EventLogging set slf4j")
      EventLogger.setImplementation(new SLF4JLogAdapter())
      println("END shared EventLogging set slf4j")
    }
    EventLogger.getImplementation
  }
  /**
   * Starts a new event context. Usually this method is not the one you want,
   * more likely you're looking for [[withContext(String)]], which will manage
   * the disposal/exit of the event context as well. Event contexts created with
   * [[enter(String)]] must be manually closed.
   * @param context name of the new context to enter
   * @return the created event context
   */
  def enter(context: String): EventContext = EventContext.enter(context)

  /**
   * Creates a new event context and runs the given block using that context. After
   * running the block, the context is closed.
   *
   * @param context name of the context to create
   * @param logErrors if true, any errors that occur in the block will be logged with the [[error()]] method
   * @param block code to run in the new context
   * @tparam T result type of the block
   * @return the return value of the block
   */
  def withContext[T](context: String, logErrors: Boolean = true)(block: => T): T = {
    require(context != null, "event context name cannot be null")
    require(context.trim() != "", "event context name must have non-whitespace characters")
    val ctx = EventContext.enter(context.trim())
    try {
      block
    }
    catch {
      case NonFatal(e) => {
        if (logErrors) {
          val message = safeMessage(e)
          error(message, exception = e)
        }
        throw e
      }
    }
    finally {
      ctx.close()
    }
  }

  private def safeMessage[T](e: Throwable): String = {
    e.getMessage match {
      case null => e.getClass.getName + " (null error message)"
      case "" => e.getClass.getName + " (empty error message)"
      case m => m
    }
  }

  /**
   * Runs the block, logging any errors that occur.
   *
   * @param block code to run
   * @tparam T return type of the block
   * @return the return value of the block
   */
  def logErrors[T](block: => T): T = {
    try {
      block
    }
    catch {
      case NonFatal(e) => {
        error(safeMessage(e), exception = e)
        throw e
      }
    }
  }

  /**
   * Throws an IllegalArgumentException with the given message
   * @param message the exception message
   * @return no return - throws IllegalArgumentException.
   * @throws IllegalArgumentException
   */
  def illegalArg(message: String) = throw new IllegalArgumentException(message)

  /**
   * Constructs an event using the provided arguments. Usually it is preferable to use one of the
   * more specific methods such as [[debug()]], [[error()]] and so on, but this method is provided
   * for the sake of completeness.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param severity indication of the importance of this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   */
  def event(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            severity: Severity = Severity.DEBUG,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = {
    var builder = EventContext.event(severity, messageCode, message, substitutions.toArray: _*)
    if (exception != null) {
      builder = builder.addException(exception)
    }
    for (m <- markers) {
      builder = builder.addMarker(m)
    }
    EventLogger.log(builder.build())
  }

  /**
   * Constructs a DEBUG level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def debug(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = event(message, messageCode, markers, Severity.DEBUG, substitutions, exception)

  /**
   * Constructs an INFO level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def info(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.INFO, substitutions, exception)

  /**
   * Constructs a WARN level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def warn(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.WARN, substitutions, exception)

  /**
   * Constructs an ERROR level event using the provided arguments.
   *
   * @param message log or error message
   * @param messageCode a numeric code for standardized error lookups
   * @param markers tags associated with this event
   * @param substitutions strings that should be substituted into the event message
   * @param exception the [[Throwable]] associated with this event, if any
   *
   */
  def error(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = event(message, messageCode, markers, Severity.ERROR, substitutions, exception)
}

