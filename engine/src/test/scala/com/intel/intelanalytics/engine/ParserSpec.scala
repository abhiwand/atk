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
/*
* Unit test specs for Parser
*/
import org.specs2.mutable.Specification
import com.intel.intelanalytics.engine.Parser

class ParserSpec extends Specification {
  "Parser" should {
    "parse a String" in {
      Parser.parse_csv("a,b") == List("a","b")      
    }
    "Parser" should{
    	"parse a String" in {
    		Parser.parse_csv("foo and bar,bar and foo,'foo, is bar'") == List("foo and bar", "bar and foo", "'foo, is bar'")
    	}
    }
  }
}