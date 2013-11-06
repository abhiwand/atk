package mapreduce;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.JDOMException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import util.CommandLineOptions;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for the ParseXml class. Verify command line options and test the xml parsing of the mapper
 *
 * @see ParseXml
 */
public class ParseXmlTest {
    private static final String[] HAPPY_COMMAND_LINE_ARGUMENTS = {"-i", "input table", "-o", "output table", "-c",
            "family:qualifier", "-d"};
    private Mapper.Context context = null;
    private Counter counter = null;
    private static final String OPTION_EXISTS = "make sure option %s exists";
    private static final String OPTION_IS_PARSED = "make sure option %s is parsed";
    private static final String VERIFY_OPTION_EXCEPTION = "verify the exception is for option %s";
    private static final String NO_EXCEPTION = "Need exception none thrown";

    @Before
    public void setUp() {
        context = mock(Mapper.Context.class);
        counter = mock(Counter.class);
        setCounterMocks();
    }

    @After
    public void tearDown() {
        context = null;
        counter = null;
    }

    /**
     * make sure an exception is not thrown when the json object is bad. If we are parsing large amounts of data we
     * don't want to stop the potentially log running job because a field had bad json
     *
     * @throws IOException
     */
    @Test
    public void parse_xml_mapper_invalid_xml() throws IOException {
        byte[][] familyQualifier = KeyValue.parseColumn(Bytes.toBytes("testing:test"));
        String test_key = "12345";
        String test_data = "<project";
        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes("12345"));

        ArrayList<KeyValue> list = new ArrayList<KeyValue>();
        KeyValue k1 = new KeyValue(Bytes.toBytes(test_key), Bytes.toBytes("testing"), Bytes.toBytes("test"), Bytes.toBytes(test_data));
        list.add(k1);
        Result result = new Result(list);

        ParseXml.ParseMapper mapper = new ParseXml.ParseMapper();
        mapper.setColumnFamily(familyQualifier[0]);

        //this should complete successfully even with busted xml. we need to keep the job running
        mapper.map(key, result, context);
    }

    @Test
    public void parse_xml_mapper_verify_xml_parsing() throws JDOMException, IOException, InterruptedException,
            IntrospectionException {
        byte[][] familyQualifier = KeyValue.parseColumn(Bytes.toBytes("testing:test"));
        String test_key = "12345";
        String test_data = "<project>" +
                "  <modelVersion>4.0.0</modelVersion>" +
                "  <artifactId>base</artifactId>" +
                "  <groupId>com.conversion</groupId>" +
                "  <version>1.0</version>" +
                "  <relativePath>../pom.xml</relativePath>" +
                "</project>";
        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes("12345"));
        Put put = new Put(key.get());
        put.add(familyQualifier[0], Bytes.toBytes("modelVersion"), Bytes.toBytes("4.0.0"));
        put.add(familyQualifier[0], Bytes.toBytes("artifactId"), Bytes.toBytes("base"));
        put.add(familyQualifier[0], Bytes.toBytes("groupId"), Bytes.toBytes("com.conversion"));
        put.add(familyQualifier[0], Bytes.toBytes("version"), Bytes.toBytes("1.0"));
        put.add(familyQualifier[0], Bytes.toBytes("relativePath"), Bytes.toBytes("../pom.xml"));

        ArrayList<KeyValue> list = new ArrayList<KeyValue>();
        KeyValue k1 = new KeyValue(Bytes.toBytes(test_key), Bytes.toBytes("testing"), Bytes.toBytes("test"), Bytes.toBytes(test_data));
        list.add(k1);
        Result result = new Result(list);

        ParseXml.ParseMapper mapper = new ParseXml.ParseMapper();
        mapper.setColumnFamily(familyQualifier[0]);
        mapper.map(key, result, context);
        //set up my argument captures for the key and put that gets written to context.write()
        ArgumentCaptor<ImmutableBytesWritable> argumentCaptorKey = ArgumentCaptor.forClass(ImmutableBytesWritable.class);
        ArgumentCaptor<Put> argumentCaptorPut = ArgumentCaptor.forClass(Put.class);
        //the very simple verify(context).write(key, put) didn't work when trying to very the call to context.write()
        //so i get the arguments that were used in the call and verify them later
        verify(context).write(argumentCaptorKey.capture(), argumentCaptorPut.capture());
        assertTrue("verify the same key was written back", argumentCaptorKey.getValue().equals(key));
        //equals didn't work with the put object so i verify the Puts family map. it's a map of all the row data
        assertTrue("verify we have the same keys in both put objects", argumentCaptorPut.getValue().getFamilyMap().equals(put.getFamilyMap()));
        //for the captured put object make sure the family, qualifier and the value exist in our created baseline put
        // object
        for (Map.Entry<byte[], List<KeyValue>> entry : argumentCaptorPut.getValue().getFamilyMap().entrySet()) {
            for (KeyValue kv : entry.getValue()) {
                String msg = String.format("couldn't find in our put test object family:%s  Qualifier:%s value:%s ", Bytes.toString(kv.getFamily()),
                        Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
                assertTrue(msg, put.has(kv.getFamily(), kv.getQualifier(), kv.getValue()));
            }
        }
    }

    /**
     * Catch the exception and verify it was thrown for the options we left out
     */
    @Test
    public final void required_option_i_throws_exception() {
        String[] commandLineArguments = {"input", "-o", "output", "-c", "column"};
        CommandLine cmd = null;
        try {
            cmd = ParseXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "i"), CommandLineOptions.lookForOptionException(e, "i"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test
    public final void required_option_o_throws_exception() throws Throwable {
        String[] commandLineArguments = {"-i", "input", "o", "output", "-c", "column"};
        CommandLine cmd = null;
        try {
            cmd = ParseXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "o"), CommandLineOptions.lookForOptionException(e, "o"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test
    public final void required_option_c_throws_exception() throws ParseException {
        String[] commandLineArguments = {"-i", "input", "-o", "output", "c", "column"};
        CommandLine cmd = null;
        try {
            cmd = ParseXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "c"), CommandLineOptions.lookForOptionException(e, "c"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test(expected = UnrecognizedOptionException.class)
    public final void option_random_throws_exception() throws ParseException {
        String[] commandLineArguments = {"-i", "input", "-o", "output", "-c", "column", "-x"};
        ParseXml.getCommandLineOptions().parseArgs(commandLineArguments);
    }

    /**
     * verify our options are getting set and parsed correctly
     *
     * @throws ParseException
     */
    @Test
    public final void option_i_is_parsed() throws ParseException {
        CommandLine cmd = ParseXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "i"), cmd.hasOption("i"));
        assertEquals(String.format(OPTION_IS_PARSED, "i"), "input table", cmd.getOptionValue("i"));
    }

    @Test
    public final void option_o_is_parsed() throws ParseException {
        CommandLine cmd = ParseXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "o"), cmd.hasOption("o"));
        assertEquals(String.format(OPTION_IS_PARSED, "o"), "output table", cmd.getOptionValue("o"));
    }

    @Test
    public final void option_c_is_parsed() throws ParseException {
        CommandLine cmd = ParseXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "c"), cmd.hasOption("c"));
        assertEquals(String.format(OPTION_IS_PARSED, "c"), "family:qualifier", cmd.getOptionValue("c"));
    }

    @Test
    public final void option_d_is_parsed() throws ParseException {
        CommandLine cmd = ParseXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "d"), cmd.hasOption("d"));
    }

    @Test
    public final void set_config_verify_config() throws IOException {
        Configuration conf = ParseXml.setConfig(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertEquals("see if conf.input is set", "input table", conf.get("conf.input"));
        assertEquals("see if conf.output is set", "output table", conf.get("conf.output"));
        assertEquals("see if conf.columnfamily is set", "family", conf.get("conf.columnfamily"));
        assertEquals("see if conf.columnqualifier is set", "qualifier", conf.get("conf.columnqualifier"));
        assertEquals("see if conf.debug is set", "true", conf.get("conf.debug"));
    }

    private void setCounterMocks() {
        when(context.getCounter(ParseXml.Counters.ROWS)).thenReturn(counter);
        when(context.getCounter(ParseXml.Counters.COLS)).thenReturn(counter);
        when(context.getCounter(ParseXml.Counters.VALID)).thenReturn(counter);
        when(context.getCounter(ParseXml.Counters.ERROR)).thenReturn(counter);
    }
}
