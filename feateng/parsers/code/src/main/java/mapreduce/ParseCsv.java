package mapreduce;

// cc ParseRdf MapReduce job that parses the raw data into separate columns (map phase only).
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;

public class ParseCsv {

  private static final Log LOG = LogFactory.getLog(ParseCsv.class);

  public static final String NAME = "ParseCsv";
  public enum Counters { ROWS, COLS, ERROR, VALID }

  /**
   * Implements the <code>Mapper</code> that reads the data and extracts the
   * required information.
   */
  static class ParseMapper
  extends TableMapper<ImmutableBytesWritable, Writable> {

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
     * @param row The row key.
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
          //value = kv.getValue().toString();
          Configuration conf = context.getConfiguration();

          String[] tokens = value.toString().split(conf.get("delimiter"),-1);
          String[] headerInfo = conf.get("headers").split(",");
          if(tokens.length!= headerInfo.length) 
                  throw new IllegalArgumentException("Header info mismatch");

          //value = Bytes.toStringBinary(kv.getValue());
          for(int i = 0 ; i< tokens.length; i++){
                  put.add(columnFamily, Bytes.toBytes(headerInfo[i]),Bytes.toBytes(tokens[i]) );
          }

        }

        context.write(row, put);
        context.getCounter(Counters.VALID).increment(1);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.info("error parsing line ");
        LOG.info("skipping line");
        
        System.err.println("Error: " + e.getMessage() + ", Row: " +
          Bytes.toStringBinary(row.get()) + ", Csv: " + value);
        context.getCounter(Counters.ERROR).increment(1);
        LOG.info("failed lines "+ context.getCounter(Counters.ERROR) );
      }
    }
  
  }

  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the parameters fails.
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    Option o = new Option("i", "input", true,
      "table to read from (must exist)");
    o.setArgName("input-table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("o", "output", true,
      "table to write to (must exist)");
    o.setArgName("output-table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("c", "column", true,
    	      "column to read data from (must exist)");
    o.setArgName("family:qualifier");
    options.addOption(o);
    
    o = new Option("h", "header", true,
    		"header (must exist)");
    o.setArgName("header");
    o.setRequired(true);
    options.addOption(o);
    
    options.addOption("g", "delimiter", true, "default delim is ,");
  
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
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
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();
    CommandLine cmd = parseArgs(otherArgs);
    // check debug flag and other options
    if (cmd.hasOption("d")) conf.set("conf.debug", "true");
    String delimiter = null;
    if (cmd.hasOption("g")){
    	delimiter = cmd.getOptionValue("g");
    }
    else
    	delimiter = ",";
    conf.set("delimiter", delimiter);
    
    conf.set("headers",cmd.getOptionValue("h"));
    // get details
    String input = cmd.getOptionValue("i");
    String output = cmd.getOptionValue("o");
    String column = cmd.getOptionValue("c");

    Scan scan = new Scan();
    if (column != null) {
      byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
      if (colkey.length > 1) {
        scan.addColumn(colkey[0], colkey[1]);
        conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0])); 
        conf.set("conf.columnqualifier", Bytes.toStringBinary(colkey[1]));
      } else {
        scan.addFamily(colkey[0]);
        conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
      }
    }

    // vv ParseRdf

    Job job = new Job(conf, "Parse data in " + input + ", write to " + output +
      "(map only)");
    job.setJarByClass(ParseCsv.class);
    TableMapReduceUtil.initTableMapperJob(input, scan, ParseMapper.class,
      ImmutableBytesWritable.class, Put.class, job);
    TableMapReduceUtil.initTableReducerJob(output,
      IdentityTableReducer.class, job);
    job.setNumReduceTasks(0);
   

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
