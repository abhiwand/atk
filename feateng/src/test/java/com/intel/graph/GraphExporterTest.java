package com.intel.graph;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TextOutputFormat.class})
public class GraphExporterTest {

    @Test
    public void getResultFolder_no_job_step() {
        boolean isExceptionRaised = false;
        FileStatus[] fileStatuses = new FileStatus[]{};
        try {
            GraphExporter.getResultFolder(fileStatuses);
        } catch(Exception e) {
            System.out.println(e.toString());
            isExceptionRaised = true;
        }
        assertEquals(isExceptionRaised, true);
    }

    @Test
    public void getResultFolder_single_job_step() {
        FileStatus status1 = mock(FileStatus.class);
        Path path1 = mock(Path.class);
        when(path1.getName()).thenReturn("job-0");
        when(status1.getPath()).thenReturn(path1);
        FileStatus[] fileStatuses = new FileStatus[]{status1};
        Path path = GraphExporter.getResultFolder(fileStatuses);
        assertEquals(path.getName(), "job-0");
    }

    @Test
    public void getResultFolder_two_job_step() {
        FileStatus status0 = mock(FileStatus.class);
        Path path0 = mock(Path.class);
        when(path0.getName()).thenReturn("job-0");
        when(status0.getPath()).thenReturn(path0);

        FileStatus status1 = mock(FileStatus.class);
        Path path1 = mock(Path.class);
        when(path1.getName()).thenReturn("job-1");
        when(status1.getPath()).thenReturn(path1);

        FileStatus[] fileStatuses = new FileStatus[]{status0, status1};
        Path path = GraphExporter.getResultFolder(fileStatuses);
        assertEquals(path.getName(), "job-1");
    }

    @Test
    public void makeElement_getEdge() {
        IGraphElementFactory factory = new TitanFaunusGraphElementFactory();
        IGraphElement element = factory.makeElement("[e[64007973][4-edge->6000264], {etl-cf:edge_type=tr, etl-cf:weight=4}]\n");
        assertEquals(element.getElementType(), GraphElementType.Edge);
        assertEquals(64007973, element.getId());
        assertEquals("tr", element.getAttributes().get("etl-cf:edge_type"));
        assertEquals("4", element.getAttributes().get("etl-cf:weight"));
        EdgeElement edge = (EdgeElement)element;
        assertEquals(4, edge.getOutVertexId());
        assertEquals(6000264, edge.getInVertexId());
    }


    @Test
    public void makeElement_getVertex() {
        IGraphElementFactory factory = new TitanFaunusGraphElementFactory();
        IGraphElement element = factory.makeElement("[v[800316], {_gb_ID=-164, etl-cf:vertex_type=R}]");
        assertEquals(element.getElementType(), GraphElementType.Vertex);
        assertEquals(800316, element.getId());
        assertEquals("-164", element.getAttributes().get("_gb_ID"));
        assertEquals("R", element.getAttributes().get("etl-cf:vertex_type"));
    }

    @Test
    public void getKeyTypesMapping() throws ParserConfigurationException, SAXException, IOException {
        String schemaXML = "<?xml version=\"1.0\" ?><schema><feature attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"etl-cf:weight\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"_id\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"_gb_ID\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\" for=\"Vertex\"></feature></schema>";
        Reader reader = new StringReader(schemaXML);

        Map<String, String> vertexKeyTypes = new HashMap<String, String>();
        Map<String, String> edgeKeyTypes = new HashMap<String, String>();
        GraphExportReducer.getKeyTypesMapping(reader, vertexKeyTypes, edgeKeyTypes);

        assertEquals(edgeKeyTypes.get("etl-cf:edge_type"), "bytearray");
        assertEquals(edgeKeyTypes.get("etl-cf:weight"), "bytearray");
        assertEquals(edgeKeyTypes.size(), 2);

        assertEquals(vertexKeyTypes.get("_id"), "bytearray");
        assertEquals(vertexKeyTypes.get("_gb_ID"), "bytearray");
        assertEquals(vertexKeyTypes.get("etl-cf:vertex_type"), "bytearray");
        assertEquals(vertexKeyTypes.size(), 3);
    }

    @Test
    public void getStatementListFromXMLString_0_statments() throws IOException, SAXException, ParserConfigurationException {
        String xml = "<query></query>";
        List<String> statements = GraphExporter.getStatementListFromXMLString(xml);
        assertEquals(0, statements.size());
    }

    @Test
    public void getStatementListFromXMLString_2_statments() throws IOException, SAXException, ParserConfigurationException {
        String xml = "<query><statement>g.V('_gb_ID','11').out.map</statement><statement>g.V('_gb_ID','11').outE.transform('{[it,it.map()]}')</statement></query>";
        List<String> statements = GraphExporter.getStatementListFromXMLString(xml);
        assertEquals(2, statements.size());
        assertEquals("g.V('_gb_ID','11').out.map", statements.get(0));
        assertEquals("g.V('_gb_ID','11').outE.transform('{[it,it.map()]}')", statements.get(1));
    }

    @Test
    public void graphMLGeneration_whole_file() throws XMLStreamException, ParserConfigurationException, SAXException, IOException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        XMLOutputFactory xmlInputFactory = XMLOutputFactory.newInstance();
        xmlInputFactory.setProperty("escapeCharacters", false);
        XMLStreamWriter writer = xmlInputFactory.createXMLStreamWriter(f);


        String schemaXML = "<?xml version=\"1.0\" ?><schema><feature attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"etl-cf:weight\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"_id\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"_gb_ID\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\" for=\"Vertex\"></feature></schema>";
        Reader reader = new StringReader(schemaXML);

        Map<String, String> vertexKeyTypes = new HashMap<String, String>();
        Map<String, String> edgeKeyTypes = new HashMap<String, String>();
        GraphExportReducer.getKeyTypesMapping(reader, vertexKeyTypes, edgeKeyTypes);

        GraphMLWriter.writeGraphMLHeaderSection(writer, vertexKeyTypes, edgeKeyTypes);
        GraphMLWriter.writeElementData(writer, "<edge id=\"62018421\" source=\"4\" target=\"5600276\" label=\"label\"><data key=\"etl-cf:edge_type\">tr</data><data key=\"etl-cf:weight\">3</data></edge>");
        GraphMLWriter.writeElementData(writer, "<node id=\"4000284\"><data key=\"_id\">4000284</data><data key=\"_gb_ID\">-304</data><data key=\"etl-cf:vertex_type\">R</data></node>");
        GraphMLWriter.writeGraphMLEndSection(writer);
        String result = f.toString();
        String expected = "<?xml version=\"1.0\" ?><graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd\"><key id=\"_id\" for=\"node\" attr.name=\"_id\" attr.type=\"bytearray\"></key><key id=\"_gb_ID\" for=\"node\" attr.name=\"_gb_ID\" attr.type=\"bytearray\"></key><key id=\"etl-cf:vertex_type\" for=\"node\" attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\"></key><key id=\"etl-cf:edge_type\" for=\"edge\" attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\"></key><key id=\"etl-cf:weight\" for=\"edge\" attr.name=\"etl-cf:weight\" attr.type=\"bytearray\"></key><graph id=\"G\" edgedefault=\"directed\">" +
                "<edge id=\"62018421\" source=\"4\" target=\"5600276\" label=\"label\"><data key=\"etl-cf:edge_type\">tr</data><data key=\"etl-cf:weight\">3</data></edge>\n" +
                "<node id=\"4000284\"><data key=\"_id\">4000284</data><data key=\"_gb_ID\">-304</data><data key=\"etl-cf:vertex_type\">R</data></node>\n" +
                "</graph></graphml>";
        assertEquals(expected, result);
    }

    @Test
    public void writeSchemaToXML_from_mapper() throws XMLStreamException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        XMLOutputFactory xmlInputFactory = XMLOutputFactory.newInstance();
        xmlInputFactory.setProperty("escapeCharacters", false);
        XMLStreamWriter writer = xmlInputFactory.createXMLStreamWriter(f);

        GraphExportMapper mapper = new GraphExportMapper();
        Map<String, GraphElementType> propertyElementTypeMapping = new HashMap<String, GraphElementType>();
        propertyElementTypeMapping.put("etl-cf:edge_type", GraphElementType.Edge);
        propertyElementTypeMapping.put("etl-cf:weight", GraphElementType.Edge);
        propertyElementTypeMapping.put("_id", GraphElementType.Vertex);
        propertyElementTypeMapping.put("_gb_ID", GraphElementType.Vertex);
        propertyElementTypeMapping.put("etl-cf:vertex_type", GraphElementType.Vertex);
        mapper.writeSchemaToXML(writer, propertyElementTypeMapping);

        String result = f.toString();
        String expected = "<?xml version=\"1.0\" ?><schema><feature attr.name=\"_id\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"etl-cf:edge_type\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"etl-cf:weight\" attr.type=\"bytearray\" for=\"Edge\"></feature><feature attr.name=\"_gb_ID\" attr.type=\"bytearray\" for=\"Vertex\"></feature><feature attr.name=\"etl-cf:vertex_type\" attr.type=\"bytearray\" for=\"Vertex\"></feature></schema>";
        assertEquals(expected, result);
    }

    @Test
    public void collectSchemaInfo() {
        Map<String, GraphElementType> propertyElementTypeMapping = new HashMap<String, GraphElementType>();
        GraphExportMapper mapper = new GraphExportMapper();
        IGraphElement vertex = new VertexElement(1);
        Map<String, Object> vertexAttributes = new HashMap<String, Object>();
        vertexAttributes.put("vf1", 1);
        vertexAttributes.put("vf2", 1);
        vertexAttributes.put("vf3", 1);
        vertex.setAttributes(vertexAttributes);

        IGraphElement edge = new EdgeElement(2);
        Map<String, Object> edgeAttributes = new HashMap<String, Object>();
        edgeAttributes.put("ef1", 1);
        edgeAttributes.put("ef2", 1);
        edgeAttributes.put("ef3", 1);
        edge.setAttributes(edgeAttributes);

        mapper.collectSchemaInfo(vertex, propertyElementTypeMapping);
        mapper.collectSchemaInfo(edge, propertyElementTypeMapping);
        assertEquals(6, propertyElementTypeMapping.size());
        assertEquals(GraphElementType.Vertex, propertyElementTypeMapping.get("vf1"));
        assertEquals(GraphElementType.Vertex, propertyElementTypeMapping.get("vf2"));
        assertEquals(GraphElementType.Vertex, propertyElementTypeMapping.get("vf3"));
        assertEquals(GraphElementType.Edge, propertyElementTypeMapping.get("ef1"));
        assertEquals(GraphElementType.Edge, propertyElementTypeMapping.get("ef2"));
        assertEquals(GraphElementType.Edge, propertyElementTypeMapping.get("ef3"));
    }

    @Test
    public void map() throws IOException, InterruptedException {
        GraphExportMapper mapper = new GraphExportMapper();
        MapDriver<LongWritable, Text, LongWritable, Text> mapDriver = new MapDriver<LongWritable, Text, LongWritable, Text>();
        mapDriver.setMapper(mapper);

        final ByteArrayOutputStream f = new ByteArrayOutputStream();
        mapper.outputStreamGenerator = new IFileOutputStreamGenerator() {
            @Override
            public OutputStream getOutputStream(JobContext context, Path path) throws IOException {
                return f;
            }
        };

        PowerMockito.mockStatic(FileOutputFormat.class);
        when(FileOutputFormat.getOutputPath(any(JobContext.class))).thenReturn(new Path("123"));

        LongWritable key = new LongWritable(1);
        mapDriver.withInput(key, new Text("2400308\t{_id=2400308, _gb_ID=-102, etl-cf:vertex_type=R}"));
        mapDriver.addOutput(key, new Text("<node id=\"2400308\"><data key=\"_id\">2400308</data><data key=\"_gb_ID\">-102</data><data key=\"etl-cf:vertex_type\">R</data></node>"));
        mapDriver.runTest();
    }

    @Test
    public void createFile_no_existing_file() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        String fileName = "testfile";
        Path outputFilePath = new Path(fileName);
        when(fs.exists(outputFilePath)).thenReturn(false);
        GraphMLWriter.createFile(fileName, fs);
        verify(fs, never()).delete(outputFilePath, true);
        verify(fs).create(outputFilePath, true);

    }

    @Test
    public void createFile_delete_existing() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        String fileName = "testfile";
        Path outputFilePath = new Path(fileName);
        when(fs.exists(outputFilePath)).thenReturn(true);
        GraphMLWriter.createFile(fileName, fs);
        verify(fs).delete(outputFilePath, true);
        verify(fs).create(outputFilePath, true);

    }
}
