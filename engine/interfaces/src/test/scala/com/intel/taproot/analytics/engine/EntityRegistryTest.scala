///*
//// Copyright (c) 2015 Intel Corporation 
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////      http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//*/
//
//package com.intel.taproot.analytics.engine
//
//import com.intel.taproot.analytics.domain.frame.{ FrameMeta, FrameReference, FrameReferenceManagement, FrameEntityType }
//import com.intel.taproot.analytics.domain.graph.GraphEntityType
//import com.intel.taproot.analytics.engine.plugin.{ Call, Invocation }
//import com.intel.taproot.analytics.engine.spark.threading.EngineExecutionContext
//import org.scalatest.{ FlatSpec, Matchers }
//
//class EntityRegistryTest extends FlatSpec with Matchers {
//
//  implicit val invocation: Invocation = Call(null, EngineExecutionContext.global)
//
//  "Adding a second manager for the same entity" should "replace the original" in {
//    val registry = new EntityTypeRegistry
//    registry.register(FrameEntityType, FrameReferenceManagement)
//    registry.register(FrameEntityType, new MockFrameManager)
//
//    val data: MockFrameManager#D = registry.resolver.resolve[MockFrameManager#D]("ia://frames/34").get
//
//    data should not be (null)
//  }
//
//}
