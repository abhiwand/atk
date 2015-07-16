/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.spark.frame

import com.intel.taproot.analytics.domain.frame.{FrameReference, FrameEntity}
import com.intel.taproot.analytics.domain.schema.Schema
import com.intel.taproot.analytics.engine.plugin.Invocation
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd

/**
 * Interface for interacting with Frames in Spark
 */
trait SparkFrame {

  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  def entity: FrameEntity

  def name: Option[String]

  def schema: Schema

  def status: Long

  def description: Option[String]

  def rowCount: Option[Long]

  def rdd: FrameRdd

  def save(rdd: FrameRdd): SparkFrame

  def sizeInBytes: Option[Long]
}

object SparkFrame {

  implicit def sparkFrameToFrameEntity(sparkFrame: SparkFrame): FrameEntity = sparkFrame.entity

  implicit def sparkFrameToFrameReference(sparkFrame: SparkFrame): FrameReference = sparkFrame.entity.toReference
}

class SparkFrameImpl(frame: FrameReference, sc: SparkContext, sparkFrameStorage: SparkFrameStorage)(implicit invocation: Invocation) extends SparkFrame {

  override def entity: FrameEntity = sparkFrameStorage.expectFrame(frame)

  override def rowCount: Option[Long] = entity.rowCount

  override def rdd: FrameRdd = sparkFrameStorage.loadFrameData(sc, entity)

  override def description: Option[String] = entity.description

  override def name: Option[String] = entity.name

  override def save(rdd: FrameRdd): SparkFrame = {
    sparkFrameStorage.saveFrameData(frame, rdd)
    this
  }

  override def status: Long = entity.status

  override def schema: Schema = entity.schema

  override def sizeInBytes: Option[Long] = sparkFrameStorage.getSizeInBytes(entity)

}