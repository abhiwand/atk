/* Copyright (C) 2013 Intel Corporation.
*     All rights reserved.
*
 *  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
* For more about this software visit:
*      http://www.01.org/GraphBuilder
 */
package com.intel.pig.data;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * \brief TupleFactory to create tuples of type {@link PropertyGraphElementTuple}
 */
public class GBTupleFactory extends TupleFactory {

	@Override
	public Tuple newTuple() {
		return new PropertyGraphElementTuple();
	}

	@Override
	public Tuple newTuple(int size) {
		return new PropertyGraphElementTuple(size);
	}

	@Override
	public Tuple newTuple(List list) {
		return new PropertyGraphElementTuple(list);
	}

	@Override
	public Tuple newTupleNoCopy(List list) {
		return new PropertyGraphElementTuple(list);
	}

	@Override
	public Tuple newTuple(Object datum) {
		Tuple t = new PropertyGraphElementTuple(1);
		try {
			t.set(0, datum);
		} catch (ExecException e) {
			throw new RuntimeException("Unable to write to field 0 in newly "
					+ "allocated tuple of size 1!", e);
		}
		return t;
	}

	@Override
	public Class<? extends Tuple> tupleClass() {
		return PropertyGraphElementTuple.class;
	}

	@Override
	public boolean isFixedSize() {
		return false;
	}
}
