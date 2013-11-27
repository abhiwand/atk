package com.intel.graph.analytics.examples;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@MonitoredUDF(errorCallback = GenericErrorHandler.class, duration=30, timeUnit= TimeUnit.MINUTES)
public class ExtractWords extends EvalFunc<DataBag> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public Schema outputSchema(Schema input) {
        Schema tuple = new Schema();
        tuple.add(new Schema.FieldSchema("word", DataType.CHARARRAY));

        try {
            return new Schema(new Schema.FieldSchema(null, tuple, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public DataBag exec(Tuple objects) throws IOException {
        String content = (String) objects.get(0);
        DataBag b = BagFactory.getInstance().newDefaultBag();
        Document doc = Jsoup.parse(content);

        String[] tokens = doc.body().text().split("\\s+");

        for (String token : tokens) {
            if (token.matches("^[a-zA-Z]+$")) {
                b.add(tupleFactory.newTuple(token));
            }
        }

        return b;
    }
}
