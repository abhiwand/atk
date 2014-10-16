package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.GraphReference
import org.scalatest.{ FlatSpec, Matchers }

import scala.util.{ Success, Try }

class UriReferenceTest extends FlatSpec with Matchers {

  "A frame uri" should "fail to resolve when no resolvers are registered" in {
    val uri: String = "ia://frame/1"
    val expected = new FrameReference(1)
    val resolver = new EntityRegistry().resolver
    intercept[IllegalArgumentException] {
      resolver.resolve(uri).get
    }
  }

  it should "fail to resolve when no resolver is registered for frames" in {
    val uri: String = "ia://frame/1"
    val expected = new FrameReference(1)
    val registry: EntityRegistry = new EntityRegistry()
    registry.register(GraphReference)
    val resolver = registry.resolver
    intercept[IllegalArgumentException] {
      resolver.resolve(uri).get
    }
  }

  it should "resolve to a frame when the URI is correct and a resolver is registered" in {
    val uri: String = "ia://frame/1"
    val expected = Success(new FrameReference(1))
    val registry: EntityRegistry = new EntityRegistry()
    registry.register(GraphReference)
    registry.register(FrameReference)
    val resolver = registry.resolver
    resolver.resolve(uri) should be(expected)
  }

  it should "be recognized as a valid URI format when the URI is correct and a resolver is registered" in {
    val uri: String = "ia://frame/1"
    val registry: EntityRegistry = new EntityRegistry()
    registry.register(FrameReference)
    val resolver = registry.resolver
    resolver.isReferenceUriFormat(uri) should be(true)
  }

  it should "not be recognized as a valid URI format when the URI is incorrect" in {
    val uri: String = "not an URI at all"
    val registry: EntityRegistry = new EntityRegistry()
    registry.register(FrameReference)
    val resolver = registry.resolver
    resolver.isReferenceUriFormat(uri) should be(false)
  }

  "A frame's ia_uri" should "create FrameReference" in {
    val uri: String = "ia://frame/2"
    val expected = Success(new FrameReference(2))

    ReferenceResolver.resolve(uri) should be(expected)
  }

  "A graph's ia_uri and entityName" should "create GraphReference" in {
    val uri: String = "ia://graph/1"
    val expected = Success(new GraphReference(1))

    ReferenceResolver.resolve(uri) should be(expected)
  }

  "A graph's ia_uri" should "create GraphReference" in {
    val uri: String = "ia://graph/2"
    val expected = Success(new GraphReference(2))

    ReferenceResolver.resolve(uri) should be(expected)
  }

  "Incorrect uri" should "throw IllegalArgumentException" in {
    val uri: String = "ia://notaframe/2"
    intercept[IllegalArgumentException] { ReferenceResolver.resolve(uri).get }
  }

}