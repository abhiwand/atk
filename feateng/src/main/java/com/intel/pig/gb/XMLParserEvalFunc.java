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
package com.intel.pig.gb;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

public class XMLParserEvalFunc extends EvalFunc<Tuple> {

	public XMLParserEvalFunc() {
	}

	/**
	 * this method will be called for each tuple in the XML file
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		BinSedesTuple tup = (BinSedesTuple) input;
		DataByteArray xml = (DataByteArray) tup.get(0);
		Tuple t = TupleFactory.getInstance().newTuple(1);
		t.set(0, xml.size());
		System.out.println("GOT XML " + xml);
		return t;
	}
}
