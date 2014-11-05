package com.intel.graphbuilder.io;

import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Split HBase regions uniformly.
 * <p/>
 * HBase's default TableInputFormat class assigns a single mapper per region. This class uniformly
 * splits HBase regions based on a user-defined split count.
 */
public class HBaseUniformSplitter {

    //Splitting the last region is not straight-forward because the last row key in a HBase table is null
    //Assumes that the same maximum row key as the uniform splitter in org.apache.hadoop.hbase.util.RegionSplitter
    public static final byte xFF = (byte) 0xFF;
    public static final byte[] maxRowKeyBytes = new byte[]{xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};

    private final List<InputSplit> initialSplits;

    public HBaseUniformSplitter(List<InputSplit> initialSplits) {
        this.initialSplits = initialSplits;
    }


    /**
     * Create uniform input splits.
     *
     * @param requestedRegionCount Requested region count.
     * @return List of uniformly split regions
     */
    public List<InputSplit> createInputSplits(int requestedRegionCount) {
        int initialRegionCount = initialSplits.size();
        int splitsPerRegion = calculateSplitsPerRegion(requestedRegionCount, initialRegionCount);

        List<InputSplit> newSplits = new ArrayList<>();

        for (InputSplit split : initialSplits) {
            List<InputSplit> regionSplits = splitRegion((TableSplit) split, splitsPerRegion);
            newSplits.addAll(regionSplits);
        }

        return newSplits;
    }

    /**
     * Split a single region into multiple input splits that server as inputs to mappers
     *
     * @param inputSplit      Input region split
     * @param splitsPerRegion Number of splits per region
     * @return New region split
     */
    private List<InputSplit> splitRegion(TableSplit inputSplit, int splitsPerRegion) {

        byte[] startKey = inputSplit.getStartRow();
        byte[] endKey = inputSplit.getEndRow();

        List<InputSplit> regionSplits;

        if (!Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)) {
            regionSplits = createUniformSplits(inputSplit, startKey, endKey, splitsPerRegion);
        } else {
            regionSplits = createUniformSplits(inputSplit, startKey, getLastRegionKey(startKey), splitsPerRegion);

            if (regionSplits.size() > 0) {
                int lastSplitIdx = regionSplits.size() - 1;
                TableSplit lastSplit = (TableSplit) regionSplits.get(lastSplitIdx);
                TableSplit newLastSplit = new TableSplit(lastSplit.getTable(), lastSplit.getStartRow(),
                        HConstants.EMPTY_BYTE_ARRAY, lastSplit.getRegionLocation());
                regionSplits.set(lastSplitIdx, newLastSplit);
            }

        }

        return regionSplits;
    }

    /**
     * Uniformly splits region based on start and end keys.
     *
     * @param initialSplit    Initial region split
     * @param startKey        Start key of split
     * @param endKey          End key of splits
     * @param splitsPerRegion Number of splits to create
     * @return Uniform splits for region
     */
    private List<InputSplit> createUniformSplits(TableSplit initialSplit, byte[] startKey, byte[] endKey,
                                                 int splitsPerRegion) {
        List<InputSplit> splits = new ArrayList<>();

        byte[][] splitKeys = getSplitKeys(startKey, endKey, splitsPerRegion);

        if (splitKeys != null) {
            for (int i = 0; i < splitKeys.length - 1; i++) {
                TableSplit tableSplit = new TableSplit(initialSplit.getTable(), splitKeys[i],
                        splitKeys[i + 1], initialSplit.getRegionLocation());
                splits.add(tableSplit);
            }
        } else {
            Log.warn("Unable to create " + splitsPerRegion + " HBase splits/region for: "
                    + initialSplit.getTable() + "/" + initialSplit +
                    ". Will use default split.");
            splits.add(initialSplit);
        }

        return (splits);
    }


    /**
     * Calculates the number of splits per region based on the requested number of region splits.
     *
     * @param requestedRegionCount Requested region count
     * @param initialRegionCount   Initial region count
     * @return Number of splits/region = max(requestedRegionCount/initialRegionCount, 1)
     */
    private int calculateSplitsPerRegion(int requestedRegionCount, int initialRegionCount) {

        int splitsPerRegion = 1;

        if (initialRegionCount > 0 && requestedRegionCount > initialRegionCount) {
            splitsPerRegion = (int) Math.ceil(requestedRegionCount / initialRegionCount);
        }

        return splitsPerRegion;
    }

    /**
     * Get split keys based on start and end keys, and requested number of splits.
     *
     * @param startKey        Start key of split
     * @param endKey          End key of splits
     * @param splitsPerRegion Number of splits per region
     * @return Split keys for range
     */
    private byte[][] getSplitKeys(byte[] startKey, byte[] endKey, int splitsPerRegion) {
        byte[][] splitKeys = null;
        if (splitsPerRegion > 1) {
            try {
                //Bytes.split() creates X+1 splits. If you want to split the range into Y, specify Y-1.
                splitKeys = Bytes.split(startKey, endKey, true, (splitsPerRegion - 1));
            } catch (IllegalArgumentException e) {
                Log.warn("Exception while getting split keys:" + e);
            }
        }
        return splitKeys;
    }

    /**
     * @param startKey
     * @return
     * @see com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
     */
    public byte[] getLastRegionKey(byte[] startKey) {

        // Compute end key using pre-split formula in
        byte[] endKey = Bytes.toBytes(((1L << 32) - 1L));
        long maxRange = 0;

        if (Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY))

        for (InputSplit split : initialSplits) {
            TableSplit tableSplit = (TableSplit) split;
            try {
                long regionRange = Bytes.toLong(tableSplit.getEndRow()) - Bytes.toLong(tableSplit.getStartRow());
                maxRange = java.lang.Math.max(maxRange, regionRange);
            }
            catch (IllegalArgumentException e) {}
        }

        if (maxRange > 0) {
            endKey = Bytes.toBytes(Bytes.toLong(startKey) + maxRange);
        }
        else if (!Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY)) {
            if (Bytes.compareTo(startKey, endKey) > 0) { //Start key > end key
                endKey = maxRowKeyBytes;
            }
        }
        return (endKey);
    }

}
