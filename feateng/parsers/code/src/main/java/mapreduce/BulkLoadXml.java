package mapreduce;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import util.CommandLineOptions;

import java.io.IOException;

/*
 *  Load XML spanning multiple lines from hdfs and parse just load it to a single column in hbase
*/
public class BulkLoadXml {
    public static final String NAME = "BulkLoadXml";

    public enum Counters {LINES}

    private static final Log LOG = LogFactory.getLog(BulkLoadXml.class);
    private static final CommandLineOptions commandLineOptions = new CommandLineOptions();

    static {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("table")
                .withDescription("table to import into (must exist)")
                .hasArg()
                .withArgName("table-name")
                .isRequired()
                .create("t"));
        options.addOption(OptionBuilder.withLongOpt("column")
                .withDescription("column to store row data into (must exist)")
                .hasArg()
                .withArgName("family:qualifier")
                .isRequired()
                .create("c"));
        options.addOption(OptionBuilder.withLongOpt("input")
                .withDescription("the directory or file to read from (must exist)")
                .hasArg()
                .withArgName("path-in-HDFS")
                .isRequired()
                .create("i"));
        options.addOption(OptionBuilder.withLongOpt("record")
                .withDescription("specify what ties a record together (must exist)")
                .hasArg()
                .withArgName("record")
                .isRequired()
                .create("r"));
        options.addOption(OptionBuilder.withLongOpt("debug")
                .withDescription("switch on DEBUG log level")
                .create("d"));

        commandLineOptions.setOptions(options);
    }

    /**
     * Implements the <code>Mapper</code> that takes the lines from the xml input
     * and outputs <code>Put</code> instances.
     */
    // vv ImportMapper
    static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private byte[] family = null;
        private byte[] qualifier = null;

        /**
         * Prepares the column family and qualifier.
         *
         * @param context The task context.
         * @throws IOException          When an operation fails - not possible here.
         * @throws InterruptedException When the task is aborted.
         */
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String column = context.getConfiguration().get("conf.column");
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colkey[0];
            if (colkey.length > 1) {
                qualifier = colkey[1];
            }
        }

        /**
         * Maps the input.
         *
         * @param offset  The current offset into the input file.
         * @param line    The current line of the file.
         * @param context The task context.
         * @throws IOException When mapping the input fails.
         */
        // vv ImportFromXml File Mapper
        @Override
        public void map(LongWritable offset, Text line, Context context) throws IOException {
            try {
                String lineString = line.toString();
                //The row key is the MD5 hash of the line to generate a random key.
                byte[] rowkey = DigestUtils.md5(lineString);
                Put put = new Put(rowkey);
                //Store the original data in a column in the given table.
                put.add(family, qualifier, Bytes.toBytes(lineString));
                context.write(new ImmutableBytesWritable(rowkey), put);
                context.getCounter(Counters.LINES).increment(1);
            } catch (Exception e) {
                LOG.error("error bulk loading xml file" + e.getMessage());
            }
        }

        void setFamily(byte[] family) {
            this.family = family;
        }

        void setQualifier(byte[] qualifier) {
            this.qualifier = qualifier;
        }
    }

    /**
     * Parse the command line parameters using the Apache Commons CLI classes.
     * These are already part of HBase and therefore are handy to process the job specific parameters.
     * Options: -i <HDFS_INPUT> -t <HBASE_TABLE_OUTPUT> -c <COLUMN_FAMILY> -r <Start tag element identified> [-d <debug flag>]
     *
     * @param args The parameters to parse.
     * @return The parsed command line.
     */
    private static CommandLine parseArgs(String[] args) {
        CommandLine cmd = null;
        try {
            cmd = commandLineOptions.parseArgs(args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", commandLineOptions.getOptions(), true);
            System.exit(-1);
        }

        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
        }

        return cmd;
    }

    /**
     * Main entry point.
     *
     * @param args The command line parameters. (See commandline method for options)
     * @throws Exception When running the job fails.
     */
    // vv ImportFromXmlFile
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //Give the command line arguments to the generic parser first to handle "-Dxyz" properties.
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);

        // check debug flag and other options
        if (cmd.hasOption("d")) conf.set("conf.debug", "true");
        // get details
        String table = cmd.getOptionValue("t");
        String input = cmd.getOptionValue("i");
        String column = cmd.getOptionValue("c");
        String rawTag = cmd.getOptionValue("r").trim();
        // generate xml end tag based on startTag
        StringBuilder startTag = new StringBuilder();
        startTag.append("<").append(rawTag).append(">");
        StringBuffer strbuf = new StringBuffer(startTag.toString());
        String endTag = strbuf.insert(1, "/").toString();

        conf.set("conf.column", column);
        conf.set("xmlinput.start", startTag.toString());
        conf.set("xmlinput.end", endTag);
        //set serializability
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        // JobDef Define the job with the required classes
        Job job = new Job(conf, "Import from file " + input + " into table " + table);
        job.setJarByClass(BulkLoadXml.class);
        job.setMapperClass(ImportMapper.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        //This is a map only job, therefore tell the framework to bypass the reduce step.
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static CommandLineOptions getCommandLineOptions() {
        return commandLineOptions;
    }
}

