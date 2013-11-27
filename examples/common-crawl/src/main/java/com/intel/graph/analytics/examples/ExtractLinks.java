package com.intel.graph.analytics.examples;
import java.io.IOException;
import java.util.ListIterator;
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
public class ExtractLinks extends EvalFunc<DataBag> {

	private TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new FieldSchema("link", DataType.CHARARRAY));
	}

	@Override
	public DataBag exec(Tuple input) throws IOException {
		String content = (String) input.get(0);
		DataBag b = BagFactory.getInstance().newDefaultBag();
		Document doc = Jsoup.parse(content);
		ListIterator<Element> link = doc.select("a").listIterator();
        int numLinks = 0;

		while (link.hasNext()) {
			try {
				Element e = link.next();
				String href = e.attr("href");
				b.add(tupleFactory.newTuple(href));
                numLinks++;
			} catch (Throwable thr) {
			}
			if(numLinks % 50 == 0){
				reporter.progress();
            }
		}

		return b;
	}
}
