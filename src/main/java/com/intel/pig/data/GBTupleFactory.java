package com.intel.pig.data;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

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
