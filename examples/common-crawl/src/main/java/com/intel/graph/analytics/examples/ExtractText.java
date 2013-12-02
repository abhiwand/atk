package com.intel.graph.analytics.examples;
import java.io.IOException;
import java.util.ListIterator;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;



@MonitoredUDF(errorCallback = GenericErrorHandler.class, duration=30, timeUnit=TimeUnit.MINUTES)
public class ExtractText extends EvalFunc<DataBag> {
	
		private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	/**
	 * this method returns the output schema information of this UDF
	 */
		@Override
		public Schema outputSchema(Schema input) {
	        return new Schema(new FieldSchema("link", DataType.CHARARRAY));
		}
		
	 
		@Override
		public DataBag exec(Tuple input) throws IOException {
			String content = (String) input.get(0);
			DataBag b = BagFactory.getInstance().newDefaultBag();
			Document doc = Jsoup.parse(content);
			b.add(tupleFactory.newTuple(doc.text()));
			return b;
		}
	
	
}