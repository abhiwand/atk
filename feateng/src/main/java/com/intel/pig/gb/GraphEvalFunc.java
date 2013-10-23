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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GraphEvalFunc extends EvalFunc<Tuple> {

	/**
	 * this method returns the output schema information of this UDF
	 */
	@Override
	public Schema outputSchema(Schema input) {
		FieldSchema f1 = new FieldSchema("src_vertex", DataType.CHARARRAY);
		FieldSchema f2 = new FieldSchema("dest_vertex", DataType.CHARARRAY);
		FieldSchema f3 = new FieldSchema("edge_data", DataType.CHARARRAY);
		FieldSchema f4 = new FieldSchema("vertex_id", DataType.CHARARRAY);
		FieldSchema f5 = new FieldSchema("vertex_data", DataType.CHARARRAY);
		FieldSchema f6 = new FieldSchema("element_type", DataType.CHARARRAY);
		List<FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
		fields.add(f1);
		fields.add(f2);
		fields.add(f3);
		fields.add(f4);
		fields.add(f5);
		fields.add(f6);
		return new Schema(fields);
	}

	/**
	 * this method will be called for each tuple in the input file we return an
	 * edge or a vertex randomly
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		Random r = new Random();
		Tuple t = TupleFactory.getInstance().newTuple(6);

		if (r.nextBoolean()) {
			t.set(0, "src_vertex");
			t.set(1, "dest_vertex");
			t.set(2, "edge_data");
			t.set(3, "-");
			t.set(4, "-");
			t.set(5, "edge");
		} else {
			t.set(0, "-");
			t.set(1, "-");
			t.set(2, "-");
			t.set(3, "vertex_id");
			t.set(4, "vertex_data");
			t.set(5, "vertex");
		}
		return t;
	}

}
