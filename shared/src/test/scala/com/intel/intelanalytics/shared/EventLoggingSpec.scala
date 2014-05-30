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

import org.scalatest.{ FlatSpec, Matchers }
import com.intel.event._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatcher

class EventLoggingSpec extends FlatSpec with Matchers with MockitoSugar {

  val rawLogger = new EventLogging {}

  "A call to withContext" should "throw IllegalArgument when called with null context" in {
    intercept[IllegalArgumentException] {
      rawLogger.withContext(null) {
        fail()
      }
    }
  }

  it should "throw IllegalArgument when called with a whitespace context" in {
    intercept[IllegalArgumentException] {
      rawLogger.withContext(" ") {
        fail()
      }
    }
  }

  it should "trim whitespace from the context argument" in {
    rawLogger.withContext(" hello   ") {
      EventContext.getCurrent.getName should be("hello")
    }
  }

  it should "return the value of the nested block" in {
    val res = rawLogger.withContext("a") {
      3 + 4
    }
    res should be(7)
  }

  it should "log any errors if logErrors was true" in {
    var called = false
    val mocked = new EventLogging {

      override def error(message: String, messageCode: Int, markers: Seq[String],
                         substitutions: Seq[String], exception: Throwable) {
        message should be("Yikes!")
        messageCode should be(0)
        markers should be(Nil)
        substitutions should be(Nil)
        exception shouldBe an[IllegalArgumentException]
        called = true
      }

    }
    intercept[IllegalArgumentException] {
      val res = mocked.withContext("a") {
        throw new IllegalArgumentException("Yikes!")
      }
    }
    called should be(true)
  }

  it should "not log any errors if logErrors was false" in {
    var called = false
    val mocked = new EventLogging {

      override def error(message: String, messageCode: Int, markers: Seq[String],
                         substitutions: Seq[String], exception: Throwable) {
        called = true
      }

    }
    intercept[IllegalArgumentException] {
      val res = mocked.withContext("a", logErrors = false) {
        throw new IllegalArgumentException("Yikes!")
      }
    }
    called should be(false)
  }

  "A call to logErrors" should "log any errors" in {
    var called = false
    val mocked = new EventLogging {

      override def error(message: String, messageCode: Int, markers: Seq[String],
                         substitutions: Seq[String], exception: Throwable) {
        message should be("Yikes!")
        messageCode should be(0)
        markers should be(Nil)
        substitutions should be(Nil)
        exception shouldBe an[IllegalArgumentException]
        called = true
      }

    }
    intercept[IllegalArgumentException] {
      val res = mocked.logErrors {
        throw new IllegalArgumentException("Yikes!")
      }
    }
    called should be(true)
  }

  it should "provide a message for exceptions without one" in {
    var called = false
    val mocked = new EventLogging {

      override def error(message: String, messageCode: Int, markers: Seq[String],
                         substitutions: Seq[String], exception: Throwable) {
        message should be(exception.getClass.getName + " (null error message)")
        messageCode should be(0)
        markers should be(Nil)
        substitutions should be(Nil)
        exception shouldBe an[IllegalArgumentException]
        called = true
      }

    }
    intercept[IllegalArgumentException] {
      val res = mocked.logErrors {
        throw new IllegalArgumentException()
      }
    }
    called should be(true)
  }

  it should "provide a message for exceptions whose message is an empty string" in {
    var called = false
    val mocked = new EventLogging {

      override def error(message: String, messageCode: Int, markers: Seq[String],
                         substitutions: Seq[String], exception: Throwable) {
        message should be(exception.getClass.getName + " (empty error message)")
        messageCode should be(0)
        markers should be(Nil)
        substitutions should be(Nil)
        exception shouldBe an[IllegalArgumentException]
        called = true
      }

    }
    intercept[IllegalArgumentException] {
      val res = mocked.logErrors {
        throw new IllegalArgumentException("")
      }
    }
    called should be(true)
  }

  "A call to illegalArg" should "throw an IllegalArgumentException" in {
    intercept[IllegalArgumentException] {
      rawLogger.illegalArg("Hey!")
    }
  }
  "Calling warn" should "generate a WARN event" in {
    val mockLog = mock[EventLog]
    EventLogger.setImplementation(mockLog)
    rawLogger.warn("Hey!")
    verify(mockLog).log(eventWith(e => e.getSeverity == Severity.WARN))
  }

  "Calling error" should "generate an ERROR event" in {
    val mockLog = mock[EventLog]
    EventLogger.setImplementation(mockLog)
    rawLogger.error("Hey!")
    verify(mockLog).log(eventWith(e => e.getSeverity == Severity.ERROR))
  }

  "Calling info" should "generate an INFO event" in {
    val mockLog = mock[EventLog]
    EventLogger.setImplementation(mockLog)
    rawLogger.info("Hey!")
    verify(mockLog).log(eventWith(e => e.getSeverity == Severity.INFO))
  }

  "Calling debug" should "generate a DEBUG event" in {
    val mockLog = mock[EventLog]
    EventLogger.setImplementation(mockLog)
    rawLogger.debug("Hey!")
    verify(mockLog).log(eventWith(e => e.getSeverity == Severity.DEBUG))
  }

  "Calling event" should "include exceptions if passed" in {
    val mockLog = mock[EventLog]
    EventLogger.setImplementation(mockLog)
    val ex = new IllegalArgumentException()
    rawLogger.event("Hey!", exception = ex)
    verify(mockLog).log(eventWith(e => e.getSeverity == Severity.DEBUG && e.getErrors.contains(ex)))
  }

  it should "include markers if passed" in {
    val mockLog = mock[EventLog]
    EventLogger.setImplementation(mockLog)
    val ex = new IllegalArgumentException()
    rawLogger.event("Hey!", markers = List("a", "b"))
    verify(mockLog).log(eventWith(e => e.getSeverity == Severity.DEBUG
      && e.getMarkers.contains("a")
      && e.getMarkers.contains("b")))
  }

  "Calling enter" should "create a new event context with the given name" in {
    val ctx = rawLogger.enter("hello")
    try {
      ctx.getName should be("hello")
    }
    finally {
      ctx.close()
    }

  }

  def eventWith(f: Event => Boolean) = {

    org.mockito.Matchers.argThat(
      new ArgumentMatcher[Event] {
        override def matches(argument: scala.Any): Boolean = {
          val event = argument.asInstanceOf[Event]
          f(event)
        }
      }
    )
  }
}
