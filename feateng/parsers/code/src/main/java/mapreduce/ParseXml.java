package mapreduce;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import util.CommandLineOptions;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

/**
 * Parse Xml MapReduce job that parses the raw data into separate columns (map phase only). uses jdom2 to parse
 * xml
 */
public class ParseXml {
    public static final String NAME = "ParseXml";

    public enum Counters {ROWS, COLS, ERROR, VALID}

    private static final Log LOG = LogFactory.getLog(ParseXml.class);
    private static final CommandLineOptions commandLineOptions = new CommandLineOptions();

    static {
        Options options = new Options();
        OptionBuilder.hasOptionalArg();
        options.addOption(OptionBuilder.withLongOpt("input")
                .withDescription("table to read from (must exist)")
                .hasArg()
                .withArgName("input-table-name")
                .isRequired()
                .create("i"));
        options.addOption(OptionBuilder.withLongOpt("output")
                .withDescription("table to read from (must exist)")
                .hasArg()
                .withArgName("output-table-name")
                .isRequired()
                .create("o"));
        options.addOption(OptionBuilder.withLongOpt("column")
                .withDescription("column to read data from (must exist)")
                .hasArg()
                .withArgName("family:qualifier")
                .isRequired()
                .create("c"));
        options.addOption(OptionBuilder.withLongOpt("debug")
                .withDescription("switch on DEBUG log level")
                .hasOptionalArg()
                .create("d"));
        commandLineOptions.setOptions(options);
    }

    /**
     * Implements the <code>Mapper</code> that reads the data and extracts the
     * required information.
     */
    static class ParseMapper extends TableMapper<ImmutableBytesWritable, Put> {
        private byte[] columnFamily = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            columnFamily = Bytes.toBytes(context.getConfiguration().get("conf.columnfamily"));
        }

        /**
         * Maps the input.
         *
         * @param row     The row key.
         * @param columns The columns of the row.
         * @param context The task context.
         * @throws java.io.IOException When mapping the input fails.
         */
        @Override
        public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException {
            context.getCounter(Counters.ROWS).increment(1);
            String value = null;
            try {
                Put put = new Put(row.get());

                for (KeyValue kv : columns.list()) {
                    context.getCounter(Counters.COLS).increment(1);
                    value = Bytes.toStringBinary(kv.getValue());
                    // Build document model from xml(jdom)
                    SAXBuilder builder = new SAXBuilder();
                    Reader in = new StringReader(value);
                    Document doc = builder.build(in);
                    Element root = doc.getRootElement();
                    for (Element node : root.getChildren()) {
                        put.add(columnFamily, Bytes.toBytes(node.getName().trim()),
                                Bytes.toBytes(node.getText().trim()));
                    }
                }
                context.write(row, put);
                context.getCounter(Counters.VALID).increment(1);
            } catch (Exception e) {
                LOG.error("Error: " + e.getMessage() + ", Row: " + Bytes.toStringBinary(row.get()) + ", XML: " + value);
                System.err.println("Error: " + e.getMessage() + ", Row: " + Bytes.toStringBinary(row.get()) +
                        ", XML: " + value);
                context.getCounter(Counters.ERROR).increment(1);
                LOG.error("error parsing line skipping, failed lines so far " + context.getCounter(Counters.ERROR));
            }
        }

        public void setColumnFamily(byte[] columnFamily) {
            this.columnFamily = columnFamily;
        }
    }

    /**
     * Parse the command line parameters.
     * Usage Options: -i <HBASE_INPUT_TABLE> -o <HBASE_OUTPUT_TABLE> -c <INPUT_COLUMN_FAMILY> [-d <debug flag>]
     *
     * @param args The parameters to parse.
     */
    private static void parseArgs(String[] args) {
        try {
            commandLineOptions.parseArgs(args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", commandLineOptions.getOptions(), true);
            System.exit(-1);
        }
        if (commandLineOptions.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
            System.out.println("DEBUG ON");
        }
    }

    /**
     * set configuration options for the job like setting input and output hbase tables and column family and qualifier
     *
     * @param args command line arguments from the user
     * @return hadoop config ready to give to the job
     */
    protected static Configuration setConfig(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        parseArgs(otherArgs);
        // check debug flag and other options
        if (commandLineOptions.hasOption("d")) conf.set("conf.debug", "true");

        // get details
        String input = commandLineOptions.getOptionValue("i");
        conf.set("conf.input", input);
        String output = commandLineOptions.getOptionValue("o");
        conf.set("conf.output", output);
        String column = commandLineOptions.getOptionValue("c");

        if (column != null) {
            byte[][] familyQualifier = KeyValue.parseColumn(Bytes.toBytes(column));
            if (familyQualifier.length > 1) {
                //Store the column family in the configuration for later use in the mapper.
                conf.set("conf.columnfamily", Bytes.toStringBinary(familyQualifier[0]));
                conf.set("conf.columnqualifier", Bytes.toStringBinary(familyQualifier[1]));
            } else {
                conf.set("conf.columnfamily", Bytes.toStringBinary(familyQualifier[0]));
            }
        }
        return conf;
    }

    /**
     * set the hbase scanner with the given family and qualifier
     *
     * @param conf previously set hadoop config
     * @return hbase table scanner with family and qualifier set or only family
     */
    private static Scan setScanner(Configuration conf) {
        Scan scan = new Scan();
        if (conf.get("conf.columnfamily") != null && conf.get("conf.columnqualifier") != null) {
            scan.addColumn(Bytes.toBytes(conf.get("conf.columnfamily")), Bytes.toBytes(conf.get("conf.columnqualifier")));
        } else {
            scan.addFamily(Bytes.toBytes(conf.get("conf.columnfamily")));
        }
        return scan;
    }

    /**
     * Main entry point.
     *
     * @param args The command line parameters. (See commandline method for options)
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = setConfig(args);
        Scan scan = setScanner(conf);

        Job job = new Job(conf, "Parse data in " + conf.get("conf.input") + ", write to " + conf.get("conf.output") +
                "(map only)");
        job.setJarByClass(ParseXml.class);
        TableMapReduceUtil.initTableMapperJob(conf.get("conf.input"), scan, ParseMapper.class,
                ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob(conf.get("conf.output"),
                IdentityTableReducer.class, job);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static CommandLineOptions getCommandLineOptions() {
        return commandLineOptions;
    }
}