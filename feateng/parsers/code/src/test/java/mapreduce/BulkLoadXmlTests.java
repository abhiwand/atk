package mapreduce;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for the BulkLoadXml class. Currently the unit test cases verify the command line options and an
 * exception and happy path for the mapper
 *
 * @see BulkLoadXml
 */
public class BulkLoadXmlTests {
    private static final String[] HAPPY_COMMAND_LINE_ARGUMENTS = {"-t", "table", "-c", "column", "-i", "input", "-r",
            "record", "-d"};
    private static final String OPTION_EXISTS = "make sure option %s exists";
    private static final String OPTION_IS_PARSED = "make sure option %s is parsed";
    private static final String VERIFY_OPTION_EXCEPTION = "verify the exception is for option %s";
    private static final String NO_EXCEPTION = "Need exception none thrown";
    private Mapper.Context context = null;
    private Counter counter = null;

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
     * We should get any parsing errors since it doesn't check for valid xml
     */
    @Test
    public void bulk_load_xml_import_invalid_xml() throws IOException, InterruptedException,
            IntrospectionException {
        byte[][] familyQualifier = KeyValue.parseColumn(Bytes.toBytes("testing:test"));
        String testData = "<sample this is my sample xml text to use with my unit test sample>";
        byte[] testKey = DigestUtils.md5(testData);
        ImmutableBytesWritable key = new ImmutableBytesWritable(testKey);
        Put put = new Put(testKey);
        put.add(familyQualifier[0], familyQualifier[1], Bytes.toBytes(testData));

        BulkLoadXml.ImportMapper mapper = new BulkLoadXml.ImportMapper();
        mapper.setFamily(familyQualifier[0]);
        mapper.setQualifier(familyQualifier[1]);
        mapper.map(new LongWritable(0), new Text(testData), context);
        //set up my argument captures for the key and put that gets written to context.write() in the mapper
        ArgumentCaptor<ImmutableBytesWritable> argumentCaptorKey = ArgumentCaptor.forClass(ImmutableBytesWritable.class);
        ArgumentCaptor<Put> argumentCaptorPut = ArgumentCaptor.forClass(Put.class);
        //the simple verify(context).write(key,put) didn't work so i get the arguments that were sent to the mocked
        //variable and verify them later.
        verify(context).write(argumentCaptorKey.capture(), argumentCaptorPut.capture());
        assertTrue("verify the same key was written back", argumentCaptorKey.getValue().equals(key));
        //equals didn't work with the put object so i verify the Puts family map. it's a map of all the row data
        assertTrue("verify we have the same keys in both put objects", argumentCaptorPut.getValue().getFamilyMap()
                .equals(put.getFamilyMap()));
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


    @Test
    public void bulk_load_xml_import_mapper_verify_write() throws JDOMException, IOException, InterruptedException,
            IntrospectionException {
        byte[][] familyQualifier = KeyValue.parseColumn(Bytes.toBytes("testing:test"));
        String testData = "<sample>this is my sample xml text to use with my unit test</sample>";
        byte[] testKey = DigestUtils.md5(testData);
        ImmutableBytesWritable key = new ImmutableBytesWritable(testKey);
        Put put = new Put(testKey);
        put.add(familyQualifier[0], familyQualifier[1], Bytes.toBytes(testData));

        BulkLoadXml.ImportMapper mapper = new BulkLoadXml.ImportMapper();
        mapper.setFamily(familyQualifier[0]);
        mapper.setQualifier(familyQualifier[1]);
        mapper.map(new LongWritable(0), new Text(testData), context);
        //set up my argument captures for the key, put that gets written to context.write() in the mapper
        ArgumentCaptor<ImmutableBytesWritable> argumentCaptorKey = ArgumentCaptor.forClass(ImmutableBytesWritable.class);
        ArgumentCaptor<Put> argumentCaptorPut = ArgumentCaptor.forClass(Put.class);
        //the simple verify(context).write(key,put) didn't work so i get the arguments that were sent to the mocked
        //variable and verify them later.
        verify(context).write(argumentCaptorKey.capture(), argumentCaptorPut.capture());

        assertTrue("verify the same key was written back", argumentCaptorKey.getValue().equals(key));
        //equals didn't work with the put object so i verify the Puts family map. it's a map of all the row data
        assertTrue("verify we have the same keys in both put objects", argumentCaptorPut.getValue().getFamilyMap()
                .equals(put.getFamilyMap()));
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
    public final void required_option_t_throws_exception() {
        String[] commandLineArguments = {"t", "table", "-c", "column", "-i", "input", "-r", "record", "-d"};
        CommandLine cmd = null;
        try {
            cmd = BulkLoadXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "t"), CommandLineOptions.lookForOptionException(e, "t"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test
    public final void required_option_c_throws_exception() {
        String[] commandLineArguments = {"-t", "table", "c", "column", "-i", "input", "-r", "record", "-d"};
        CommandLine cmd = null;
        try {
            cmd = BulkLoadXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "c"), CommandLineOptions.lookForOptionException(e, "c"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test
    public final void required_option_i_throws_exception() {
        String[] commandLineArguments = {"-t", "table", "-c", "column", "i", "input", "-r", "record", "-d"};
        CommandLine cmd = null;
        try {
            cmd = BulkLoadXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "i"), CommandLineOptions.lookForOptionException(e, "i"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test
    public final void required_option_r_throws_exception() {
        String[] commandLineArguments = {"-t", "table", "-c", "column", "-i", "input", "r", "record", "-d"};
        CommandLine cmd = null;
        try {
            cmd = BulkLoadXml.getCommandLineOptions().parseArgs(commandLineArguments);
        } catch (ParseException e) {
            assertTrue(String.format(VERIFY_OPTION_EXCEPTION, "r"), CommandLineOptions.lookForOptionException(e, "r"));
        }
        if (cmd != null) fail(NO_EXCEPTION);
    }

    @Test(expected = UnrecognizedOptionException.class)
    public final void option_random_throws_exception() throws ParseException {
        String[] commandLineArguments = {"-t", "table", "-c", "column", "-i", "input", "r", "record", "-d", "-x"};
        BulkLoadXml.getCommandLineOptions().parseArgs(commandLineArguments);
    }

    @Test
    public final void option_t_is_parsed() throws ParseException {
        CommandLine cmd = BulkLoadXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "t"), cmd.hasOption("t"));
        assertEquals(String.format(OPTION_IS_PARSED, "t"), "table", cmd.getOptionValue("t"));
    }

    @Test
    public final void option_c_is_parsed() throws ParseException {
        CommandLine cmd = BulkLoadXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "c"), cmd.hasOption("c"));
        assertEquals(String.format(OPTION_IS_PARSED, "c"), "column", cmd.getOptionValue("c"));
    }

    @Test
    public final void option_i_is_parsed() throws ParseException {
        CommandLine cmd = BulkLoadXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "i"), cmd.hasOption("i"));
        assertEquals(String.format(OPTION_IS_PARSED, "i"), "input", cmd.getOptionValue("i"));
    }

    @Test
    public final void option_r_is_parsed() throws ParseException {
        CommandLine cmd = BulkLoadXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "r"), cmd.hasOption("r"));
        assertEquals(String.format(OPTION_IS_PARSED, "r"), "record", cmd.getOptionValue("r"));
    }

    @Test
    public final void option_d_is_parsed() throws ParseException {
        CommandLine cmd = BulkLoadXml.getCommandLineOptions().parseArgs(HAPPY_COMMAND_LINE_ARGUMENTS);
        assertTrue(String.format(OPTION_EXISTS, "d"), cmd.hasOption("d"));
    }

    private void setCounterMocks() {
        when(context.getCounter(BulkLoadXml.Counters.LINES)).thenReturn(counter);
    }
}
