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

package com.intel.intelanalytics.engine

//The parser object
object Parser{
	 // function to split a string in different columns
	 def parse_csv(s: String): List[String] = {
	 	//pattern to match 
	 	val ptn = "(('[^']*')|([^,]+))".r
	 	//find pattern and create a list of string
	 	return  ptn.findAllMatchIn(s).map(_.toString).toList
	 }

	 def main(args: Array[String]){
	 	val test = "foo and bar,bar and foo,'foo, is bar'"
	 	val row = parse_csv(test)
	 	
	 	
	 	for (i <- 0 to row.length-1){
	 		println(row(i))
	 	}
	 }
}
