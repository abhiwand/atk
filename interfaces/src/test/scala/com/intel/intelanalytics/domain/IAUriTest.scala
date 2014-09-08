//package com.intel.intelanalytics.domain
//
//import com.intel.intelanalytics.domain.frame.{FrameReference, DataFrame}
//import com.intel.intelanalytics.domain.graph.GraphReference
//import org.scalatest.{WordSpec, Matchers, FlatSpec}
//
//class IAUriTest extends FlatSpec with  Matchers {
// "A frame's ia_uri and entityName" should "create FrameReference" in {
//   val uri : String = "ia://frame/1"
//   val entityName : Option[String] = Some("frame")
//   val expected = new FrameReference(1)
//
//   IAUriFactory.getReference(uri,entityName) should be (expected)
// }
//
//  "A frame's ia_uri" should "create FrameReference" in {
//    val uri : String = "ia://frame/2"
//    val expected = new FrameReference(2)
//
//    IAUriFactory.getReference(uri) should be (expected)
//  }
//
//  "A graph's ia_uri and entityName" should "create GraphReference" in {
//    val uri : String = "ia://graph/1"
//    val entityName : Option[String] = Some("graph")
//    val expected = new GraphReference(1)
//
//    IAUriFactory.getReference(uri, entityName) should be (expected)
//  }
//
//  "A graph's ia_uri" should "create GraphReference" in {
//    val uri : String = "ia://graph/2"
//    val expected = new GraphReference(2)
//
//    IAUriFactory.getReference(uri) should be (expected)
//  }

//  "Mismatch in uri and entityName" should "throw IllegalArgumentException" in {
//    val uri : String = "ia://frame/1"
//    val entityName : Option[String] = Some("graph")
//    intercept[IllegalArgumentException]
//    IAUriFactory.getReference(uri,entityName)
//  }
//
//  "Incorrect uri" should "throw IllegalArgumentException" in {
//    val uri : String = "ia://notaframe/2"
//    intercept[IllegalArgumentException]
//    IAUriFactory.getReference(uri)
//  }

//}