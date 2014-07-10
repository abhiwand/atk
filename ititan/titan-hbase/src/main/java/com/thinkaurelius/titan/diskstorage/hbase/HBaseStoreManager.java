package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.nio.ByteBuffer;

import static com.thinkaurelius.titan.diskstorage.Backend.EDGESTORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.ID_STORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.EDGEINDEX_STORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.VERTEXINDEX_STORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static com.thinkaurelius.titan.diskstorage.Backend.SYSTEM_PROPERTIES_STORE_NAME;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStoreManager.class);

    public static final String TABLE_NAME_KEY = "tablename";
    public static final String TABLE_NAME_DEFAULT = "titan";


    public static final String SHORT_CF_NAMES_KEY = "short-cf-names";
    public static final boolean SHORT_CF_NAMES_DEFAULT = false;

    public static final int PORT_DEFAULT = 9160;

    public static final String HBASE_CONFIGURATION_NAMESPACE = "hbase-config";

    public static final String SKIP_SCHEMA_CHECK = "skip-schema-check";
    public static final boolean SKIP_SCHEMA_CHECK_DEFAULT = false;

    public static final ImmutableMap<String, String> HBASE_CONFIGURATION;

    static {
        HBASE_CONFIGURATION = new ImmutableMap.Builder<String, String>()
                .put(GraphDatabaseConfiguration.HOSTNAME_KEY, "hbase.zookeeper.quorum")
                .put(GraphDatabaseConfiguration.PORT_KEY, "hbase.zookeeper.property.clientPort")
                .build();
    }

    /**
     * Related bug fixed in 0.98.0, 0.94.7, 0.95.0:
     * <p/>
     * https://issues.apache.org/jira/browse/HBASE-8170
     */
    public static final int MIN_REGION_COUNT = 3;

    /**
     * The total number of HBase regions to create with Titan's table. This
     * setting only effects table creation; this normally happens just once when
     * Titan connects to an HBase backend for the first time.
     */

    public static final String REGION_COUNT =  "region-count";
//    public static final ConfigOption<Integer> REGION_COUNT = new ConfigOption<Integer>(STORAGE_NS, "region-count",
//            "The number of initial regions set when creating Titan's HBase table",
//            ConfigOption.Type.MASKABLE, Integer.class, new Predicate<Integer>() {
//        @Override
//        public boolean apply(Integer input) {
//            return null != input && MIN_REGION_COUNT <= input;
//        }
//    }
//    );

    /**
     * This setting is used only when {@link #REGION_COUNT} is unset.
     * <p/>
     * If Titan's HBase table does not exist, then it will be created with total
     * region count = (number of servers reported by ClusterStatus) * (this
     * value).
     * <p/>
     * The Apache HBase manual suggests an order-of-magnitude range of potential
     * values for this setting:
     * <p/>
     * <ul>
     * <li>
     * <a href="https://hbase.apache.org/book/important_configurations.html#disable.splitting">2.5.2.7. Managed Splitting</a>:
     * <blockquote>
     * What's the optimal number of pre-split regions to create? Mileage will
     * vary depending upon your application. You could start low with 10
     * pre-split regions / server and watch as data grows over time. It's
     * better to err on the side of too little regions and rolling split later.
     * </blockquote>
     * </li>
     * <li>
     * <a href="https://hbase.apache.org/book/regions.arch.html">9.7 Regions</a>:
     * <blockquote>
     * In general, HBase is designed to run with a small (20-200) number of
     * relatively large (5-20Gb) regions per server... Typically you want to
     * keep your region count low on HBase for numerous reasons. Usually
     * right around 100 regions per RegionServer has yielded the best results.
     * </blockquote>
     * </li>
     * </ul>
     * <p/>
     * These considerations may differ for other HBase implementations (e.g. MapR).
     */
    public static final String REGIONS_PER_SERVER = "regions-per-server";
    //public static final ConfigOption<Integer> REGIONS_PER_SERVER = new ConfigOption<Integer>(STORAGE_NS, "regions-per-server",
    //        "The number of regions per regionserver to set when creating Titan's HBase table",
    //        ConfigOption.Type.MASKABLE, Integer.class);

    private static final byte[] START_KEY;
    private static final byte[] END_KEY;

    static {
        // 4 bytes = 32 bits > 30 bit partition width
        START_KEY = new byte[4];
        END_KEY = new byte[4];

        for (int i = 0; i < START_KEY.length; i++)
            START_KEY[i] = (byte) 0;

        for (int i = 0; i < END_KEY.length; i++)
            END_KEY[i] = (byte) -1;
    }


    private final String tableName;
    private final org.apache.hadoop.conf.Configuration hconf;
    private final int regionCount;
    private final int regionsPerServer;

    private final ConcurrentMap<String, HBaseKeyColumnValueStore> openStores;
    private final HTablePool connectionPool;

    private final StoreFeatures features;

    private final boolean shortCfNames;
    private static final BiMap<String, String> shortCfNameMap =
            ImmutableBiMap.<String, String>builder()
                    .put(VERTEXINDEX_STORE_NAME, "v")
                    .put(ID_STORE_NAME, "i")
                    .put(EDGESTORE_NAME, "s")
                    .put(EDGEINDEX_STORE_NAME, "e")
                    .put(VERTEXINDEX_STORE_NAME + LOCK_STORE_SUFFIX, "w")
                    .put(ID_STORE_NAME + LOCK_STORE_SUFFIX, "j")
                    .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "t")
                    .put(EDGEINDEX_STORE_NAME + LOCK_STORE_SUFFIX, "f")
                    .put(SYSTEM_PROPERTIES_STORE_NAME, "c")
                    .build();

    static {
        // Verify that shortCfNameMap is injective
        // Should be guaranteed by Guava BiMap, but it doesn't hurt to check
        Preconditions.checkArgument(null != shortCfNameMap);
        Collection<String> shorts = shortCfNameMap.values();
        Preconditions.checkArgument(Sets.newHashSet(shorts).size() == shorts.size());
    }

    private org.apache.commons.configuration.Configuration config = null;

    public HBaseStoreManager(org.apache.commons.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        this.config = config;

        this.tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);


        this.hconf = HBaseConfiguration.create();
        for (Map.Entry<String, String> confEntry : HBASE_CONFIGURATION.entrySet()) {
            if (config.containsKey(confEntry.getKey())) {
                hconf.set(confEntry.getValue(), config.getString(confEntry.getKey()));
            }
        }

        // Copy a subset of our commons config into a Hadoop config
        org.apache.commons.configuration.Configuration hbCommons = config.subset(HBASE_CONFIGURATION_NAMESPACE);

        @SuppressWarnings("unchecked") // I hope commons-config eventually fixes this
                Iterator<String> keys = hbCommons.getKeys();
        int keysLoaded = 0;

        while (keys.hasNext()) {
            String key = keys.next();
            String value = hbCommons.getString(key);
            logger.debug("HBase configuration: setting {}={}", key, value);
            hconf.set(key, value);
            keysLoaded++;
        }

        logger.debug("HBase configuration: set a total of {} configuration values", keysLoaded);

        connectionPool = new HTablePool(hconf, connectionPoolSize);

        this.shortCfNames = config.getBoolean(SHORT_CF_NAMES_KEY, SHORT_CF_NAMES_DEFAULT);

        //this.regionCount = config.has(REGION_COUNT) ? config.getInteger(REGION_COUNT, -1) : -1;
        this.regionCount =  config.getInt(REGION_COUNT, -1);

        //this.regionsPerServer = config.has(REGIONS_PER_SERVER) ? config.get(REGIONS_PER_SERVER) : -1;
        this.regionsPerServer = config.getInt(REGIONS_PER_SERVER, -1);

        /*
         * Specifying both region count options is permitted but may be
         * indicative of a misunderstanding, so issue a warning.
         */
        if (config.containsKey(REGIONS_PER_SERVER) && config.containsKey(REGION_COUNT)) {
            logger.warn("Both {} and {} are set in Titan's configuration, but "
                            + "the former takes precedence and the latter will be ignored.",
                    REGION_COUNT, REGIONS_PER_SERVER);
        }
        openStores = new ConcurrentHashMap<String, HBaseKeyColumnValueStore>();

        // TODO: allowing publicly mutate fields is bad, should be fixed
        features = new StoreFeatures();
        features.supportsOrderedScan = true;
        features.supportsUnorderedScan = true;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsMultiQuery = true;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;
    }


    @Override
    public String toString() {
        return "hbase[" + tableName + "@" + super.toString() + "]";
    }

    @Override
    public void close() {
        openStores.clear();
    }


    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        //TODO: use same timestamp functionality as Cassandra
//        final Timestamp timestamp = super.getTimestamp(txh);
//        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = convertToCommands(mutations, timestamp.additionTime, timestamp.deletionTime);

        final long delTS = System.currentTimeMillis();
        final long putTS = delTS + 1;
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = convertToCommands(mutations, putTS, delTS);
        List<Row> batch = new ArrayList<Row>(commandsPerKey.size()); // actual batch operation

        // convert sorted commands into representation required for 'batch' operation
        for (Pair<Put, Delete> commands : commandsPerKey.values()) {
            if (commands.getFirst() != null)
                batch.add(commands.getFirst());

            if (commands.getSecond() != null)
                batch.add(commands.getSecond());
        }

        try {
            HTableInterface table = null;

            try {
                table = connectionPool.getTable(tableName);
                table.batch(batch);
                table.flushCommits();
            } finally {
                IOUtils.closeQuietly(table);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } catch (InterruptedException e) {
            throw new TemporaryStorageException(e);
        }

        waitUntil(putTS);
    }

    @Override
    public KeyColumnValueStore openDatabase(final String longName) throws StorageException {

        HBaseKeyColumnValueStore store = openStores.get(longName);

        if (store == null) {

            final String cfName = shortCfNames ? shortenCfName(longName) : longName;

            HBaseKeyColumnValueStore newStore = new HBaseKeyColumnValueStore(this, connectionPool, tableName, cfName, longName);

            store = openStores.putIfAbsent(longName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) {
                if (!this.config.getBoolean(SKIP_SCHEMA_CHECK, SKIP_SCHEMA_CHECK_DEFAULT))
                    ensureColumnFamilyExists(tableName, cfName);

                store = newStore;
            }
        }

        return store;
    }

    private String shortenCfName(String longName) throws PermanentStorageException {
        final String s;
        if (shortCfNameMap.containsKey(longName)) {
            s = shortCfNameMap.get(longName);
            Preconditions.checkNotNull(s);
            logger.debug("Substituted default CF name \"{}\" with short form \"{}\" to reduce HBase KeyValue size", longName, s);
        } else {
            if (shortCfNameMap.containsValue(longName)) {
                String fmt = "Must use CF long-form name \"%s\" instead of the short-form name \"%s\" when configured with %s=true";
                String msg = String.format(fmt, shortCfNameMap.inverse().get(longName), longName, SHORT_CF_NAMES_KEY);
                throw new PermanentStorageException(msg);
            }
            s = longName;
            logger.debug("Kept default CF name \"{}\" because it has no associated short form", s);
        }
        return s;
    }

    private HTableDescriptor ensureTableExists(String tableName) throws StorageException {
        HBaseAdmin adm = getAdminInterface();

        HTableDescriptor desc;

        try { // Create our table, if necessary
            if (adm.tableExists(tableName)) {
                desc = adm.getTableDescriptor(tableName.getBytes());
            } else {
                desc = createTable(tableName, adm);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }

        return desc;
    }

    private HTableDescriptor createTable(String name, HBaseAdmin adm) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);

        int count; // total regions to create
        String src;

        if (MIN_REGION_COUNT <= (count = regionCount)) {
            src = "region count configuration";
        } else if (0 < regionsPerServer && MIN_REGION_COUNT <= (count = regionsPerServer * getServerCount(adm))) {
            src = "ClusterStatus server count";
        } else {
            count = -1;
            src = "default";
        }

        if (MIN_REGION_COUNT < count) {
            adm.createTable(desc, getStartKey(count), getEndKey(count), count);
            logger.debug("Created table {} with region count {} from {}", tableName, count, src);
        } else {
            adm.createTable(desc);
            logger.debug("Created table {} with default start key, end key, and region count", tableName);
        }

        return desc;
    }

    /**
     * This method generates the second argument to
     * {@link HBaseAdmin#createTable(HTableDescriptor, byte[], byte[], int)}.
     * <p/>
     * From the {@code createTable} javadoc:
     * "The start key specified will become the end key of the first region of
     * the table, and the end key specified will become the start key of the
     * last region of the table (the first region has a null start key and
     * the last region has a null end key)"
     * <p/>
     * To summarize, the {@code createTable} argument called "startKey" is
     * actually the end key of the first region.
     */
    private byte[] getStartKey(int regionCount) {
        ByteBuffer regionWidth = ByteBuffer.allocate(4);
        regionWidth.putInt((int) (((1L << 32) - 1L) / regionCount)).flip();
        return StaticArrayBuffer.of(regionWidth).getBytes(0, 4);
    }

    /**
     * Companion to {@link #getStartKey(int)}. See its javadoc for details.
     */
    private byte[] getEndKey(int regionCount) {
        ByteBuffer regionWidth = ByteBuffer.allocate(4);
        regionWidth.putInt((int) (((1L << 32) - 1L) / regionCount * (regionCount - 1))).flip();
        return StaticArrayBuffer.of(regionWidth).getBytes(0, 4);
    }

    private void ensureColumnFamilyExists(String tableName, String columnFamily) throws StorageException {
        HBaseAdmin adm = getAdminInterface();
        HTableDescriptor desc = ensureTableExists(tableName);

        Preconditions.checkNotNull(desc);

        HColumnDescriptor cf = desc.getFamily(columnFamily.getBytes());

        // Create our column family, if necessary
        if (cf == null) {
            try {
                adm.disableTable(tableName);
                desc.addFamily(new HColumnDescriptor(columnFamily).setCompressionType(Compression.Algorithm.GZ));
                adm.modifyTable(tableName.getBytes(), desc);

                try {
                    logger.debug("Added HBase ColumnFamily {}, waiting for 1 sec. to propogate.", columnFamily);
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    throw new TemporaryStorageException(ie);
                }

                adm.enableTable(tableName);
            } catch (TableNotFoundException ee) {
                logger.error("TableNotFoundException", ee);
                throw new PermanentStorageException(ee);
            } catch (org.apache.hadoop.hbase.TableExistsException ee) {
                logger.debug("Swallowing exception {}", ee);
            } catch (IOException ee) {
                throw new TemporaryStorageException(ee);
            }
        } else { // check if compression was enabled, if not - enable it
            if (cf.getCompressionType() == null || cf.getCompressionType() == Compression.Algorithm.NONE) {
                try {
                    adm.disableTable(tableName);

                    adm.modifyColumn(tableName, cf.setCompressionType(Compression.Algorithm.GZ));

                    adm.enableTable(tableName);
                } catch (IOException e) {
                    throw new TemporaryStorageException(e);
                }
            }
        }
    }

    @Override
    public StoreTransaction beginTransaction(final StoreTxConfig config) throws StorageException {
        return new HBaseTransaction(config);
    }


    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     */
    @Override
    public void clearStorage() throws StorageException {
        HBaseAdmin adm = getAdminInterface();

        try { // first of all, check if table exists, if not - we are done
            if (!adm.tableExists(tableName)) {
                logger.debug("clearStorage() called before table {} was created, skipping.", tableName);
                return;
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }

        HTable table = null;

        try {
            table = new HTable(hconf, tableName);

            Scan scan = new Scan();
            scan.setBatch(100);
            scan.setCacheBlocks(false);
            scan.setCaching(2000);

            ResultScanner scanner = null;

            try {
                scanner = table.getScanner(scan);

                for (Result res : scanner) {
                    table.delete(new Delete(res.getRow()));
                }
            } finally {
                IOUtils.closeQuietly(scanner);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public String getName() {
        return tableName;
    }

    private HBaseAdmin getAdminInterface() {
        try {
            return new HBaseAdmin(hconf);
        } catch (IOException e) {
            throw new TitanException(e);
        }
    }

    /**
     * Convert Titan internal Mutation representation into HBase native commands.
     *
     * @param mutations    Mutations to convert into HBase commands.
     * @param putTimestamp The timestamp to use for Put commands.
     * @param delTimestamp The timestamp to use for Delete commands.
     * @return Commands sorted by key converted from Titan internal representation.
     * @throws PermanentStorageException
     */
    private Map<StaticBuffer, Pair<Put, Delete>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                                                                   final long putTimestamp,
                                                                   final long delTimestamp) throws PermanentStorageException {
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = new HashMap<StaticBuffer, Pair<Put, Delete>>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {

            String cfString = getCfNameForStoreName(entry.getKey());
            byte[] cfName = cfString.getBytes();

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                StaticBuffer key = m.getKey();
                KCVMutation mutation = m.getValue();

                Pair<Put, Delete> commands = commandsPerKey.get(key);

                if (commands == null) {
                    commands = new Pair<Put, Delete>();
                    commandsPerKey.put(key, commands);
                }

                if (mutation.hasDeletions()) {
                    if (commands.getSecond() == null)
                        commands.setSecond(new Delete(key.as(StaticBuffer.ARRAY_FACTORY), delTimestamp)); //, null));

                    for (StaticBuffer b : mutation.getDeletions()) {
                        commands.getSecond().deleteColumns(cfName, b.as(StaticBuffer.ARRAY_FACTORY), delTimestamp);
                    }
                }

                if (mutation.hasAdditions()) {
                    if (commands.getFirst() == null)
                        commands.setFirst(new Put(key.as(StaticBuffer.ARRAY_FACTORY), putTimestamp));

                    for (Entry e : mutation.getAdditions()) {
                        commands.getFirst().add(cfName,
                                e.getArrayColumn(),
                                putTimestamp,
                                e.getArrayValue());
                    }
                }
            }
        }

        return commandsPerKey;
    }

    private int getServerCount(HBaseAdmin adm) {
        int serverCount = -1;
        try {
            serverCount = adm.getClusterStatus().getServers().size();
            logger.debug("Read {} servers from HBase ClusterStatus", serverCount);
        } catch (IOException e) {
            logger.debug("Unable to retrieve HBase cluster status", e);
        }
        return serverCount;
    }

    private String getCfNameForStoreName(String storeName) throws PermanentStorageException {
        return shortCfNames ? shortenCfName(storeName) : storeName;
    }

    private static void waitUntil(long until) {
        long now = System.currentTimeMillis();

        while (now <= until) {
            try {
                Thread.sleep(1L);
                now = System.currentTimeMillis();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
