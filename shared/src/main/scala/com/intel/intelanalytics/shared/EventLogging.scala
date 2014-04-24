package com.intel.intelanalytics.shared

import com.intel.event.{EventLogger, Severity, EventContext}

trait EventLogging {

  def enter(context: String) = EventContext.enter(context)

  def withContext[T](context: String, logErrors: Boolean = true) (block: => T) : T = {
    val ctx = EventContext.enter(context)
    try {
      block
    } catch {
      case e: Exception => {error(e.getMessage, exception = e); throw e}
    } finally {
      ctx.close()
    }
  }

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


