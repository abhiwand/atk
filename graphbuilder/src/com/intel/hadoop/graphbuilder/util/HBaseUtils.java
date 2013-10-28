
package com.intel.hadoop.graphbuilder.util;

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;

public class HBaseUtils {

    private static HBaseUtils uniqueInstanceOfHBaseUtils = null;

    private static final Logger LOG = Logger.getLogger(HBaseUtils.class);

    private Configuration configuration;
    private HBaseAdmin    admin;
    private String        hTableName;
    private HTable        hTable;

    /**
     *  return the unique instance of HBaseUtils, create one if there isn't one already
     *
     *  @throws IOException
     *
     */

    public static synchronized HBaseUtils getInstance() throws IOException {

        if (uniqueInstanceOfHBaseUtils == null) {
                uniqueInstanceOfHBaseUtils = new HBaseUtils();
        }

        return uniqueInstanceOfHBaseUtils;
    }

    /**
     * Return a new configuration
     */
    public static Configuration getNewConfiguration() {
        return HBaseConfiguration.create();
    }

    /**
     * Parse the column name to return the family and qualifier in a string array
     *
     * @param columnName Column name in HBase "column family:column qualifier"
     */
    public static byte[][] parseColumnName(String columnName) {
        return KeyValue.parseColumn(Bytes.toBytes(columnName));
    }

    /**
     * Get the cell value from HBase given a column handler and column name
     *
     * @param columns        Scanned columns from a HTable row passed from a mapper/reducer
     * @param fullColumnName Full column key "family:qualifier"
     */
    public static byte[] getColumnData(Result columns, String fullColumnName) {
        byte[][] columnKey = HBaseUtils.parseColumnName(fullColumnName);

        if (columnKey.length < 2) {
            return null;
        }

        // Read cell from HTable

        return columns.getValue(columnKey[0], columnKey[1]);
    }

    /**
     * Get the column value from HBase given a table handler, rowkey, and column key
     *
     * @param Key           Row key
     * @param colFamilyName Column family name
     * @param colName       Column name
     */

    public byte[] getColumnData(String Key, String colFamilyName, String colName)
            throws IOException, NumberFormatException {

        byte[] rowKey     = Bytes.toBytes(Key);
        Get    getRowData = new Get(rowKey);
        Result res        = hTable.get(getRowData);

        return res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
    }

    /**
     * Write a cell value to HBase
     *
     * @param key             HBase row key
     * @param columnFamily    HBase column family (default - "VertexID")
     * @param columnQualifier HBase column name
     * @param value
     * @param context         Context of Hadoop's reducer
     */
    public static byte[] putValue(byte[] key,
                                  byte[] columnFamily,
                                  byte[] columnQualifier,
                                  byte[] value,
                                  Reducer.Context context) throws IOException, InterruptedException {
        Put put = new Put(key);
        put.add(columnFamily, columnQualifier, value);
        context.write(new Text(GBHTableConfig.config.getProperty("NULLKEY")), put);
        return value;
    }

    /**
     * Check if the table exists in HBase
     *
     * @param hTableName HBase table name
     */
    public boolean tableExists(String hTableName) throws IOException {
        return admin.tableExists(hTableName);
    }

    /**
     * Constructor method
     *
     * @throws IOException
     */
    private HBaseUtils() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.admin         = new HBaseAdmin(configuration);
    }

    /**
     * Constructor method
     *
     * @throws IOException
     */
    private HBaseUtils(String tableName) throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.hTableName    = tableName;
        this.hTable        = new HTable(configuration, hTableName);
    }

    /**
     * Return configuration
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }

    /**
     * Create a HBase table
     *
     * @param hTableName           Name of the HBase table to be created
     * @param hTableColumnFamilies Names of the table column families
     */
    public Scan createTable(String hTableName, String[] hTableColumnFamilies) throws IOException {

        Scan scan = getTableScanner(hTableName);

        // Delete the vertex ID temporary table and create it again for this job

        if (admin.tableExists(this.hTableName)) {

            admin.disableTable(this.hTableName);

            if (admin.isTableDisabled(this.hTableName)) {
                admin.deleteTable(this.hTableName);
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.HBASE_ERROR,
                        "GRAPHBUILDER ERROR: Unable to delete existing table " + this.hTableName + ". Please delete it",
                        LOG);
            }
        }

        HTableDescriptor htd = new HTableDescriptor(this.hTableName);

        for (String colDesc : hTableColumnFamilies) {
            HColumnDescriptor hcd = new HColumnDescriptor(colDesc);
            htd.addFamily(hcd);
        }

        admin.createTable(htd);
        LOG.info("TRIBECA_INFO: " + this.hTableName + " table created");

        if (!admin.tableExists(this.hTableName)) {
            throw new IOException("GRAPHBUILDER ERROR: Failed to create table " + this.hTableName);
        }

        return scan;
    }

    /**
     * Create a HBase table with a single column family
     *
     * @param hTableName         Name of the HBase table to be created
     * @param hTableColumnFamily Names of the table column family
     */
    public Scan createTable(String hTableName, String hTableColumnFamily) throws IOException {

        Scan scan = getTableScanner(hTableName);

        // Delete the vertex ID temporary table and create it again for this job

        if (admin.tableExists(this.hTableName)) {

            admin.disableTable(this.hTableName);

            if (admin.isTableDisabled(this.hTableName)) {
                admin.deleteTable(this.hTableName);
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.HBASE_ERROR,
                        "GRAPHBUILDER ERROR: Unable to delete existing table " + this.hTableName + ". Please delete it",
                        LOG);
            }
        }

        HTableDescriptor  htd = new HTableDescriptor(this.hTableName);
        HColumnDescriptor hcd = new HColumnDescriptor(hTableColumnFamily);

        htd.addFamily(hcd);

        admin.createTable(htd);

        LOG.info("TRIBECA_INFO: " + this.hTableName + " table created");

        if (!admin.tableExists(this.hTableName)) {
            throw new IOException("GRAPHBUILDER ERROR: Failed to create table " + this.hTableName);
        }

        return scan;
    }

    public Scan getTableScanner(String tableName) {
        this.hTableName = tableName;
        Scan scan       = new Scan();

        scan.setCaching(GBHTableConfig.config.getPropertyInt("HBASE_CACHE_SIZE"));
        scan.setCacheBlocks(false);

        return scan;
    }
}