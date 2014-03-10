package mapreduce;

import org.apache.commons.cli.*;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import util.CommandLineOptions;

import java.io.IOException;

/**
 * ParseJson2 MapReduce job that parses the raw data into separate columns (map phase only). uses
 * org.json.simple.parser.JSONParser
 *
 * @see org.json.simple.parser.JSONParser
 */
public class ParseJson {
    public static final String NAME = "ParseJson";

    public enum Counters {ROWS, COLS, ERROR, VALID}

    private static final Log LOG = LogFactory.getLog(ParseJson.class);
    private static final CommandLineOptions commandLineOptions = new CommandLineOptions();

    static {
        Options options = new Options();
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
                .create("d"));
        commandLineOptions.setOptions(options);
    }

    /**
     * Implements the <code>Mapper</code> that reads the data and extracts the
     * required information.
     */
    static class ParseMapper extends TableMapper<ImmutableBytesWritable, Put> {
        private JSONParser parser = new JSONParser();
        private byte[] columnFamily = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            columnFamily = Bytes.toBytes(
                    context.getConfiguration().get("conf.columnfamily"));
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
        public void map(ImmutableBytesWritable row, Result columns, Context context)
                throws IOException {
            context.getCounter(Counters.ROWS).increment(1);
            String value = null;
            try {
                Put put = new Put(row.get());
                for (KeyValue kv : columns.list()) {
                    context.getCounter(Counters.COLS).increment(1);
                    value = Bytes.toStringBinary(kv.getValue());
                    JSONObject json = (JSONObject) parser.parse(value);
                    for (Object key : json.keySet()) {
                        Object val = json.get(key);
                        put.add(columnFamily, Bytes.toBytes(key.toString()),
                                Bytes.toBytes(val.toString()));
                    }
                }
                context.write(row, put);
                context.getCounter(Counters.VALID).increment(1);
            } catch (Exception e) {
                LOG.error("Error: " + e.getMessage() + ", Row: " + Bytes.toStringBinary(row.get()) + ", JSON: " + value);
                System.err.println("Error: " + e.getMessage() + ", Row: " + Bytes.toStringBinary(row.get()) +
                        ", JSON: " + value);
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
     *
     * @param args The parameters to parse.
     * @return The parsed command line.
     */
    private static CommandLine parseArgs(String[] args) {
        CommandLine cmd = null;
        //catch only specific exception we expect
        try {
            cmd = commandLineOptions.parseArgs(args);
        } catch (ParseException e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", commandLineOptions.getOptions(), true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
            System.out.println("DEBUG ON");
        }
        return cmd;
    }

    /**
     * set configuration options for the job like setting input and output hbase tables and column family and qualifier
     *
     * @param args command line arguments from the user
     * @return hadoop config ready to give to the job
     */
    public static Configuration setConfig(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);
        // check debug flag and other options
        if (cmd.hasOption("d")) conf.set("conf.debug", "true");

        // get details
        String input = cmd.getOptionValue("i");
        String output = cmd.getOptionValue("o");
        String column = cmd.getOptionValue("c");
        Scan scan = new Scan();
        if (column != null) {
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            if (colkey.length > 1) {
                scan.addColumn(colkey[0], colkey[1]);
                // co ParseJson2-2-Conf Store the column family in the configuration for later use in the mapper.
                conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
                conf.set("conf.columnqualifier", Bytes.toStringBinary(colkey[1]));
            } else {
                scan.addFamily(colkey[0]);
                conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
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
     * @param args The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = setConfig(args);
        Scan scan = setScanner(conf);

        // vv ParseJson
        Job job = new Job(conf, "Parse data in " + conf.get("conf.input") + ", write to " + conf.get("conf.output") +
                "(map only)");
        job.setJarByClass(ParseJson.class);
        TableMapReduceUtil.initTableMapperJob(conf.get("conf.input"), scan, ParseMapper.class, ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob(conf.get("conf.output"), IdentityTableReducer.class, job);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static CommandLineOptions getCommandLineOptions() {
        return commandLineOptions;
    }
}
