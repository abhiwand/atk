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
package com.tribeca.etl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * @author nyigitba
 * 
 */
public class BagTransformColToRow extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple t) throws IOException {
		DataBag bag = (DataBag) t.get(0);
		String fieldName = (String) t.get(1);
		// need to create a new bag otherwise we get:
		// java.lang.IllegalStateException: InternalCachedBag is closed for
		// adding new tuples
		DataBag result = BagFactory.getInstance().newDefaultBag();

		Iterator<Tuple> it = bag.iterator();
		Double transformationValue = null;
		
		while (it.hasNext()) {
			Tuple tuple = it.next();
			// copy fields except the 0th (key) and the 3th (transformed value)
			Tuple existingTuple = TupleFactory.getInstance().newTuple(2);// field_name,current_val
			// skip the 0th field, which is the key introduced by the GROUP
			// operator
			existingTuple.set(0, tuple.get(1));
			existingTuple.set(1, tuple.get(2));
			result.add(existingTuple);

			Object value = tuple.get(3);// key,field_name,current_val,transformed_value
			if (value != null && !((String) value).equals("")) {
				transformationValue = Double.valueOf((String) value);
			}

		}
		
		Tuple transformedValueTuple = TupleFactory.getInstance()
				.newTuple(2);
		transformedValueTuple.set(0, fieldName);
		transformedValueTuple.set(1, transformationValue);
		result.add(transformedValueTuple);
		return result;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema("values", DataType.BAG));
	}
}
