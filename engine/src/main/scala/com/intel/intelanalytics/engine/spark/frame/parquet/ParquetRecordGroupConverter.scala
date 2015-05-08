//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame.parquet

import parquet.io.api.{ Converter, GroupConverter }

/**
 *  Class used by the Parquet libraries to convert a group of objects into a group of primitive objects
 */
class ParquetRecordGroupConverter extends GroupConverter {
  /** called at the beginning of the group managed by this converter */
  override def start(): Unit = {}
  /**
   * call at the end of the group
   */
  override def end(): Unit = {}

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of the field in this group
   * @return the corresponding converter
   */
  override def getConverter(fieldIndex: Int): Converter = {
    new ParquetRecordConverter()
  }
}
