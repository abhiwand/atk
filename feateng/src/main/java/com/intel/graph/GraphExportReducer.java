package com.intel.graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Reducer first writes the header section of the graphml. It then puts the xml content for each element (coming from mapper)
 * to the graphml. It writes the end section of the graph in the end.
 */
public class GraphExportReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private FSDataOutputStream outputStream;
    XMLStreamWriter writer = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        outputFactory.setProperty("escapeCharacters", false);
        Configuration conf = context.getConfiguration();
        String fileName = conf.get(GraphExporter.FILE);

        try {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FileStatus[] fileStatuses = fs.listStatus(TextOutputFormat.getOutputPath(context));
            Map<String, String> edgeKeyTypes = new HashMap<String, String>();
            Map<String, String> vertexKeyTypes = new HashMap<String, String>();
            for (FileStatus status : fileStatuses) {
                String name = status.getPath().getName();
                if(name.startsWith(GraphExporter.METADATA_FILE_PREFIX)) {
                    // collects the schema info created by mappers
                    getKeyTypesMapping(new InputStreamReader(fs.open(status.getPath())), vertexKeyTypes, edgeKeyTypes);
                }
            }

            outputStream = GraphMLWriter.createFile(fileName, FileSystem.get(conf));
            writer = outputFactory.createXMLStreamWriter(outputStream, "UTF8");
            GraphMLWriter.writeGraphMLHeaderSection(writer, vertexKeyTypes, edgeKeyTypes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header");
        }
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) {

        for(Text value: values){
            GraphMLWriter.writeElementData(writer, value.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        GraphMLWriter.writeGraphMLEndSection(writer);
        try {
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to close the xml writer", e);
        } finally {
            outputStream.close();
        }
    }

    /**
     * Retrieve schema info from the file written by mapper
     * @param reader: Reader object for the file content
     * @param vertexKeyTypes: vertex schema mapping
     * @param edgeKeyTypes: edge schema mapping
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public static void getKeyTypesMapping(Reader reader, Map<String, String> vertexKeyTypes, Map<String, String> edgeKeyTypes) throws IOException, SAXException, ParserConfigurationException {

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(new InputSource(reader));

        NodeList nList = doc.getElementsByTagName(GraphExporter.FEATURE);
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Element nNode = (Element)nList.item(temp);
            String name = nNode.getAttribute(GraphMLTokens.ATTR_NAME);
            String type = nNode.getAttribute(GraphMLTokens.ATTR_TYPE);
            String elementType = nNode.getAttribute(GraphMLTokens.FOR);
            if(GraphElementType.Vertex.toString().equalsIgnoreCase(elementType))
                vertexKeyTypes.put(name, type);
            else
                edgeKeyTypes.put(name, type);
        }
    }

}
