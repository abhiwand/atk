package com.intel.graph;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

public class GraphExportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    IGraphElementFactory factory = null;
    XMLOutputFactory inputFactory = null;

    @Override
    protected void setup(Context context) {
        factory = new TitanFaunusGraphElementFactory();
        inputFactory = XMLOutputFactory.newInstance();
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        try {
            XMLStreamWriter writer = inputFactory.createXMLStreamWriter(f);
            IGraphElement element = factory.makeElement(value.toString());
            element.writeToXML(writer);
            context.write(key, new Text(f.toString()));
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to generate xml node for the element");
        }
    }
}

