package io.bigdime.alert.multiple.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;
import org.apache.hadoop.hbase.client.Put;

public class HBaseLogTask implements Callable<Object> {
	private final HbaseManager hbaseManager;
	private final String hbaseTableName;
	private final List<Put> puts;

	public HBaseLogTask(HbaseManager _hbaseManager, String _hbaseTableName, List<Put> puts) {
		hbaseManager = _hbaseManager;
		hbaseTableName = _hbaseTableName;
		this.puts = puts;
	}

	public HBaseLogTask(HbaseManager _hbaseManager, String _hbaseTableName, Put put) {
		hbaseManager = _hbaseManager;
		hbaseTableName = _hbaseTableName;
		puts = new ArrayList<>(1);
		puts.add(put);
	}

	@Override 
	public Object call() throws Exception {
		try {
			final DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
			final DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
					.withTableName(hbaseTableName).withtPuts(puts).build();

			hbaseManager.insertData(dataInsertionSpecification);
		} catch (IOException | HBaseClientException e) {
			e.printStackTrace(System.err);// what else to do
		}
		return null;
	}
}
