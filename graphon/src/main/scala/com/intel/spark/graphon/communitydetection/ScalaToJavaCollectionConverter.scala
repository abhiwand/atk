
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

package com.intel.spark.graphon.communitydetection

import scala.collection.JavaConversions._

/**
 * Scala collection to java collection converter with serialization
 */
object ScalaToJavaCollectionConverter extends Serializable {

  /**
   * convert the scala.collection.Set[Long] to java.util.Set[Long]
   * @param scalaSet
   * @return
   */
  def convertSet(scalaSet: Set[Long]): java.util.Set[Long] = {
    val javaSet = new java.util.HashSet[Long]()
    scalaSet.foreach(entry => javaSet.add(entry))
    javaSet
  }

}
