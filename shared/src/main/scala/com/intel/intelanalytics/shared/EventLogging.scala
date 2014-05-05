package com.intel.intelanalytics.shared

import com.intel.event.{EventLogger, Severity, EventContext}
import scala.util.control.NonFatal

trait EventLogging {

  def enter(context: String) = EventContext.enter(context)

  def withContext[T](context: String, logErrors: Boolean = true) (block: => T) : T = {
    val ctx = EventContext.enter(context)
    try {
      block
    } catch {
      case NonFatal(e) => {
        val message = safeMessage(e)
        error(message, exception = e)
        throw e
      }
    } finally {
      ctx.close()
    }
  }

  def safeMessage[T](e: Throwable): String = {
    e.getMessage match {
      case null => e.getClass.getName + " (null error message)"
      case "" => e.getClass.getName + " (empty error message)"
      case m => m
    }
  }

  def logErrors[T](block: =>T): T = {
    try {
      block
    } catch {
      case NonFatal(e) => {
        error(safeMessage(e), exception = e)
        throw e
      }
    }
  }

  def illegalArg(s: String) = throw new IllegalArgumentException(s)

  def event(message: String,
            messageCode: Int = 0,
            markers: Seq[String] = Nil,
            severity: Severity = Severity.DEBUG,
            substitutions: Seq[String] = Nil,
            exception: Throwable = null) = {
    var builder = EventContext.event(severity, messageCode, message, substitutions.toArray : _*)
    if (exception != null) {
      builder = builder.addException(exception)
    }
    for(m <- markers) {
      builder = builder.addMarker(m)
    }
    EventLogger.log(builder.build())
  }

  def debug(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.DEBUG, substitutions, exception)

  def info(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.INFO, substitutions, exception)

  def warn(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.WARN, substitutions, exception)

  def error(message: String,
           messageCode: Int = 0,
           markers: Seq[String] = Nil,
           substitutions: Seq[String] = Nil,
           exception: Throwable = null) = event(message, messageCode, markers, Severity.ERROR, substitutions, exception)
}


