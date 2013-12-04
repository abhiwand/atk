package com.intel.pig.udf;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;

public class ExtractElement extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {
		DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
		PropertyGraphElementTuple t = (PropertyGraphElementTuple) new GBTupleFactory()
				.newTuple(1);
		PropertyGraphElementStringTypeVids p = new PropertyGraphElementStringTypeVids();
		Vertex<StringType> vertex = new Vertex<StringType>(new StringType(
				"test_vid"));
		p.init(GraphElementType.VERTEX, vertex);
		byte type = DataType.findType(t);
		System.out.println(">>>type " + type);
		System.out.println(p instanceof WritableComparable);
		System.out.println(p instanceof PropertyGraphElement);
		System.out.println(p instanceof Writable);
		t.set(0, p);
		// t.set(1, p);
		// t.set(2, p);
		// bag.add(t);
		return t;
	}

	// @Override
	// public Schema outputSchema(Schema input) {
	// Schema tuple = new Schema();
	// FieldSchema f1 = new FieldSchema("gb_tuple",
	// DataType.GENERIC_WRITABLECOMPARABLE);
	// tuple.add(f1);
	// return tuple;
	// // try {
	// // return new Schema(new Schema.FieldSchema(null, tuple, DataType.BAG));
	// // } catch (Exception e) {
	// // return null;
	// // }
	// }

}
