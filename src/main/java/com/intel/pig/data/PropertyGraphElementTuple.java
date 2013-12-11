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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.AbstractTuple;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SizeUtil;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;

/**
 * \brief PropertyGraphElementTuple is the tuple type processed by the GB 2.0
 * (alpha) dataflow
 * 
 * PropertyGraphElementTuple contains a list of property graph elements.
 * Currently, GB UDFs know how to process tuples of type
 * PropertyGraphElementTuple.
 */
public class PropertyGraphElementTuple extends AbstractTuple {

	List<PropertyGraphElement> propertyGraphElements;

	public PropertyGraphElementTuple() {
		propertyGraphElements = new ArrayList<PropertyGraphElement>();
	}

	public PropertyGraphElementTuple(int size) {
		propertyGraphElements = new ArrayList<PropertyGraphElement>(size);
		for (int i = 0; i < size; i++)
			propertyGraphElements.add(null);
	}

	public PropertyGraphElementTuple(List elements) {
		propertyGraphElements = elements;
	}

	@Override
	public int size() {
		return propertyGraphElements.size();
	}

	@Override
	public Object get(int fieldNum) throws ExecException {
		return propertyGraphElements.get(fieldNum);
	}

	@Override
	public List<Object> getAll() {
		List<? extends Object> casted = propertyGraphElements;
		return (List<Object>) casted;
	}

	@Override
	public void set(int fieldNum, Object val) throws ExecException {
		// TODO do type checks
		propertyGraphElements.set(fieldNum, (PropertyGraphElement) val);

	}

	@Override
	public void append(Object val) {
		// TODO do type checks
		propertyGraphElements.add((PropertyGraphElement) val);
	}

	/*
	 * copied from DefaultTuple implementation
	 */
	@Override
	public long getMemorySize() {
		Iterator<PropertyGraphElement> i = propertyGraphElements.iterator();
		// fixed overhead
		long empty_tuple_size = 8 /* tuple object header */
		+ 8 /*
			 * isNull - but rounded to 8 bytes as total obj size needs to be
			 * multiple of 8
			 */
		+ 8 /* mFields reference */
		+ 32 /* mFields array list fixed size */;

		// rest of the fixed portion of mfields size is accounted within
		// empty_tuple_size
		long mfields_var_size = SizeUtil
				.roundToEight(4 + 4 * propertyGraphElements.size());
		// in java hotspot 32bit vm, there seems to be a minimum tuple size of
		// 96
		// which is probably from the minimum size of this array list
		mfields_var_size = Math.max(40, mfields_var_size);

		long sum = empty_tuple_size + mfields_var_size;
		while (i.hasNext()) {
			sum += SizeUtil.getPigObjMemSize(i.next());
		}
		return sum;
	}

	/*
	 * copied from DefaultTuple implementation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// Clear our fields, in case we're being reused.
		propertyGraphElements.clear();
		// Make sure it's a tuple.
		byte b = in.readByte();
		if (b != DataType.TUPLE) {
			int errCode = 2112;
			String msg = "Unexpected data while reading tuple "
					+ "from binary file.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}
		// Read the number of fields
		int sz = in.readInt();
		for (int i = 0; i < sz; i++) {
			try {
				append(DataReaderWriter.readDatum(in));
			} catch (ExecException ee) {
				throw ee;
			}
		}

	}

	/*
	 * copied from DefaultTuple implementation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(DataType.TUPLE);
		int sz = size();
		out.writeInt(sz);
		for (int i = 0; i < sz; i++) {
			DataReaderWriter.writeDatum(out, propertyGraphElements.get(i));
		}

	}

	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

}
