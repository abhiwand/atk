package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.command.Command
import org.joda.time.DateTime

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
