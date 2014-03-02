package com.intel.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mock;


public class TestColumnDropper {
    @Test
    public void testSplitMultipleColumnNames() {
        String colNames = "src,weight, dest";
        List<String> fields = HBaseColumnDropperMapper.splitFields(colNames);
        assertEquals("should have 3 columns", 3, fields.size());
        assertEquals("first column", "src", fields.get(0));
        assertEquals("second column", "weight", fields.get(1));
        assertEquals("third column", "dest", fields.get(2));

    }

    @Test
    public void testMapper() throws IOException {
        HBaseColumnDropperMapper mapper = new HBaseColumnDropperMapper();


        Mapper.Context context = mock(Mapper.Context.class);
        Configuration conf = mock(Configuration.class);
        PowerMockito.when(context.getConfiguration()).thenReturn(conf);
        String columnName = "f1,f2";
        String columnFamily = "etl-cf";
        PowerMockito.when(conf.get(HBaseColumnDropper.COLUMN_NAME)).thenReturn(columnName);
        PowerMockito.when(conf.get(HBaseColumnDropper.COLUMN_FAMILY)).thenReturn(columnFamily);

        ImmutableBytesWritable rowBytes = new ImmutableBytesWritable();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(10);
        rowBytes.set(buffer.array());

        mock(String.class);
        Result result = mock(Result.class);
        KeyValue kv = mock(KeyValue.class);
        KeyValue.SplitKeyValue sv = mock(KeyValue.SplitKeyValue.class);
        Long time = 11111111L;
        buffer = ByteBuffer.allocate(8);
        buffer.putLong(time);
        PowerMockito.when(kv.split()).thenReturn(sv);
        PowerMockito.when(sv.getTimestamp()).thenReturn(buffer.array());
        PowerMockito.when(result.getColumnLatest(columnFamily.getBytes(), "f1".getBytes())).thenReturn(kv);
        PowerMockito.when(result.getColumnLatest(columnFamily.getBytes(), "f2".getBytes())).thenReturn(kv);
        mapper.map(rowBytes, result, context);
        List<Delete> deleteList = (List<Delete>)Whitebox.getInternalState(mapper, "deleteList");
        assertEquals("delete 2 columns", 2, deleteList.size());
    }
}
