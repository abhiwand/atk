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

package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.command.Command
import org.joda.time.DateTime

/**
 * ComputeStatus captures the stages of execution for a demand-executed (lazy)
 * value.
 */
sealed trait ComputeStatus extends Serializable
/**
 * Computation has not yet begun
 */
case object Suspended extends ComputeStatus

/**
 * Computation is in progress
 */
case object InProgress extends ComputeStatus

/**
 * Computation has completed
 */
case object Complete extends ComputeStatus

/**
 * Things that are computed on demand (i.e. lazily)
 */
trait OnDemand {

  /**
   * The date time on which this value started being computed.
   *
   * This is not the time when it was created, it is the time when its value
   * was needed, so computation was forced.
   */
  def materializedOn: Option[DateTime]

  /**
   * The date time when materialization completed. Will be None
   * when [[materializedOn]] is None, and may be None when [[materializedOn]] is Some,
   * which would mean that computation is in progress.
   */
  def materializationComplete: Option[DateTime]

  /**
   * Describes the computation status of this value
   *
   * @see [[ComputeStatus]]
   */
  def computeStatus: ComputeStatus = (materializedOn, materializationComplete) match {
    case (None, _) => Suspended
    case (Some(_), None) => InProgress
    case (Some(_), Some(_)) => Complete
  }

  /**
   * The command that produced this object
   */
  def command(): Command
}
