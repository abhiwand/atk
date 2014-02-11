package com.intel.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;

public class HBaseDeleteRowsUDF extends EvalFunc<Tuple> {
	private final Log log = LogFactory.getLog(getClass());

	private LoadStoreCaster caster = new HBaseBinaryConverter();
	private Configuration conf = null;
	private String tableName;
	private HTable table;
	private List<Delete> deleteBatch;
	private int batchSize = 100;
	private boolean initialized = false;

	public HBaseDeleteRowsUDF(String tableName, String batchSizeStr) {
		this.tableName = tableName;
		this.batchSize = Integer.valueOf(batchSizeStr);
		deleteBatch = new ArrayList<Delete>();
	}

	private void initialize() {
		conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, this.tableName);
		} catch (IOException e) {
			log.warn("Problem opening table", e);
			throw new RuntimeException("Problem opening table", e);
		}
		log.info("initialized " + getClass().getName());
		initialized = true;
	}

	public void finish() {
		try {
			if (deleteBatch.size() > 0) {
				// some DELETEs to process
				try {
					table.delete(deleteBatch);
				} catch (Exception e) {
					log.warn("Problem during batch delete", e);
				}
			}
		} finally {
			closeTable();
		}
	}

	private void closeTable() {
		try {
			table.close();
		} catch (IOException e) {
			log.warn("Problem closing table", e);
		}
	}

	@Override
	public Tuple exec(Tuple t) throws IOException {
		if (!initialized)
			initialize();
		Object firstElement = t.get(0);
		if (firstElement == null) {
			warn("Null row key found in the first element of input tuple",
					PigWarning.UDF_WARNING_1);
			return null;
		}

		byte type = DataType.findType(firstElement);
		byte[] rowKey = PigHBaseUtils.objToBytes(firstElement, type, caster);
		Delete del = new Delete(rowKey);
		deleteBatch.add(del);
		if (deleteBatch.size() > batchSize) {
			try {
				table.delete(deleteBatch);
			} catch (Exception e) {
				// need to catch it to continue
				// processing DELETEs on partial failures
				log.warn("Problem during batch delete", e);
			}
		}
		return null;
	}
}
