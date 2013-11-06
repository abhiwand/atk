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

package models

import play.api.libs.json.{JsObject, Json}

/**
 * Define status codes
 */
package object StatusCodes {
    val ALREADY_REGISTER = 1002
    val REGISTRATION_APPROVAL_PENDING = 1003
    val NOT_YET_REGISTERED = 1004
    val REGISTERED = 1000
    val LOGIN = 1001

    val SC = Map(
        1001 -> "ipython",
        1000 -> "ipython",
        1002 -> "ipython",
        1003 -> "",
        1004 -> ""
    )

    /**
     *
     * @param code
     * @return
     */
    def getJsonStatusCode(code: Int): JsObject = {
        Json.obj("code" -> code.toString, "url" -> SC.get(code))
    }
}