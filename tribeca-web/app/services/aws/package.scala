//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package services

import play.api.Play
import play.api.Play.current
import com.amazonaws.auth.BasicAWSCredentials

package object aws {
  val baseAccessKey = Play.application.configuration.getString("aws.access_key").get
  val baseSecretAccessKey = Play.application.configuration.getString("aws.secret_access_key").get
  val baseCredentials = new BasicAWSCredentials(baseAccessKey, baseSecretAccessKey)
}

class awsUser(params:com.typesafe.config.ConfigObject){
    //val accountId = params.get(0).getString("accountId")
  val tests:String = "";
  def awsUser(params:List[_ <: com.typesafe.config.ConfigObject]) = {
    println(params.toString)
  }
}
