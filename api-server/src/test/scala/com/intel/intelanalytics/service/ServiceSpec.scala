package com.intel.intelanalytics.service

import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.mock.MockitoSugar
import spray.testkit.ScalatestRouteTest
import spray.routing.HttpService
import com.intel.event.EventLogger
import com.intel.event.adapter.SLF4JLogAdapter

/**
 * Parent class for ServiceSpecs
 */
abstract class ServiceSpec extends FlatSpec with MockitoSugar with Matchers with ScalatestRouteTest with HttpService {

  EventLogger.setImplementation(new SLF4JLogAdapter())

  override val actorRefFactory = system
}
