package com.intel.graph;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphExportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    IGraphElementFactory elementFactory = null;
    XMLOutputFactory xmlInputFactory = null;

    Map<String, GraphElementType> propertyElementTypeMapping = new HashMap<String, GraphElementType>();

    @Override
    protected void setup(Context context) {
        elementFactory = new TitanFaunusGraphElementFactory();
        xmlInputFactory = XMLOutputFactory.newInstance();
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        try {
            XMLStreamWriter writer = xmlInputFactory.createXMLStreamWriter(f);
            IGraphElement element = elementFactory.makeElement(value.toString());
            element.writeToXML(writer);
            context.write(key, new Text(f.toString()));

            // collects schema info
            Set<String> keySet = element.getAttributes().keySet();
            for(String feature : keySet) {
                propertyElementTypeMapping.put(feature, element.getElementType());
            }
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to generate xml node for the element");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        TaskAttemptID id = context.getTaskAttemptID();

        Path path = new Path(new File(TextOutputFormat.getOutputPath(context).toString(), GraphExporter.METADATA_FILE_PREFIX + id.toString()).toString());
        FileSystem fs = FileSystem.get(context.getConfiguration());

        FSDataOutputStream output = fs.create(path, true);
        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = outputFactory.createXMLStreamWriter(output, "UTF8");
            writer.writeStartDocument();
            writer.writeStartElement(GraphExporter.SCHEMA);

            for(Map.Entry<String, GraphElementType> e : propertyElementTypeMapping.entrySet()) {
                writer.writeStartElement(GraphExporter.FEATURE);
                writer.writeAttribute(GraphMLTokens.ATTR_NAME, e.getKey());
                writer.writeAttribute(GraphMLTokens.ATTR_TYPE, "bytearray");
                writer.writeAttribute(GraphMLTokens.FOR, e.getValue().toString());
                writer.writeEndElement();
            }

            writer.writeEndElement(); // schema
            writer.writeEndDocument();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to export schema info from mapper");
        }
    }
}

