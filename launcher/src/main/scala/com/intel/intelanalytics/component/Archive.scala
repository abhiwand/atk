package com.intel.intelanalytics.component

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

trait Archive extends Component with Locator {

  override def locatorName: String = this.componentName

  private var loader: Option[(String, String) => Any] = None

  /**
   * Called by the component framework to provide a method for loading new classes from the same
   * archive, with the correct startup support. Archives should store this function for later use
   */
  def setLoader(function: (String, String) => Any): Unit = loader match {
    case None => loader = Some(function)
    case _ => throw new Exception("Loader function already set for this archive")
  }

  /**
   * Called by archives in order to load new instances from the archive. Does not provide
   * any caching of instances.
   *
   * @param className the class name to instantiate and configure
   * @return the new instance
   */
  protected def load(componentName: String, className: String): Any = {
    println("Loading " + className)
    loader.getOrElse(throw new Exception("Loader not installed for this archive"))(componentName, className)
  }

}
