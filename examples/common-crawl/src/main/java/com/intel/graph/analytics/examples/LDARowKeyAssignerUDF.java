package com.intel.graph.analytics.examples;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


@MonitoredUDF(errorCallback = GenericErrorHandler.class, duration=30, timeUnit=TimeUnit.MINUTES)
public class LDARowKeyAssignerUDF extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple t) throws IOException {

		Tuple new_tuple = TupleFactory.getInstance().newTuple(5);

		String src_domain = t.get(0).toString();
		String dest_domain = t.get(1).toString();

		int randInt = new Random().nextInt(100000);
		long ts = System.currentTimeMillis();

		new_tuple.set(
				0,
				String.format("%s:%s%s", String.valueOf(randInt),
						String.valueOf(src_domain.hashCode()),
						String.valueOf(ts)));
		new_tuple.set(1, src_domain);
		new_tuple.set(2, dest_domain);
		new_tuple.set(3, t.get(2));
		new_tuple.set(4,t.get(3));
		return new_tuple;
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();

		FieldSchema f1 = new FieldSchema("key", DataType.CHARARRAY);
		FieldSchema f2 = new FieldSchema("id1", DataType.CHARARRAY);
		FieldSchema f3 = new FieldSchema("id2", DataType.CHARARRAY);
		FieldSchema f4 = new FieldSchema("num", DataType.LONG);
		FieldSchema f5 = new FieldSchema("identifier", DataType.CHARARRAY);
		

		tupleSchema.add(f1);
		tupleSchema.add(f2);
		
		tupleSchema.add(f3);
		tupleSchema.add(f4);
		tupleSchema.add(f5);
		
		try {
			return new Schema(new Schema.FieldSchema(null, tupleSchema,
					DataType.TUPLE));
		} catch (Exception e) {
			return null;
		}
	}
}
