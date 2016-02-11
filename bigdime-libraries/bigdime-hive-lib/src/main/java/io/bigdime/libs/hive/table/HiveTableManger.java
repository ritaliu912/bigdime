/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.table;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.bigdime.libs.hive.client.HiveClientProvider;
import io.bigdime.libs.hive.common.ColumnMetaDataUtil;
import io.bigdime.libs.hive.common.HiveConfigManager;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.libs.hive.metadata.TableMetaData;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.HCatTable.Type;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.springframework.util.Assert;

import com.google.common.base.Preconditions;
/**
 * 
 * @author mnamburi
 *
 */
public class HiveTableManger extends HiveConfigManager {

	private static final char defaultChar = '\u0000';

	public HiveTableManger(Properties properties) {
		super(properties);
	}

	public static HiveTableManger getInstance(){
		return getInstance(null);
	}
	
	public static HiveTableManger getInstance(Properties properties){
		return new HiveTableManger(properties);
	}
	/**
	 * 
	 * @param tableSpecfication
	 * @throws HCatException
	 */
	public void createTable(TableSpecification tableSpecfication) throws HCatException{
		HCatClient client = null;
		HCatCreateTableDesc tableDescriptor;
		HCatTable htable = new HCatTable(tableSpecfication.databaseName, tableSpecfication.tableName);
		if(tableSpecfication.isExternal){
			Preconditions.checkNotNull(tableSpecfication.location,"Location cannot be null, if table is external");
			htable.tableType(Type.EXTERNAL_TABLE);
			htable.location(tableSpecfication.location);
		}
		if (tableSpecfication.fileFormat != null) {
			htable.fileFormat(tableSpecfication.fileFormat);
		}

		if (tableSpecfication.fieldsTerminatedBy != defaultChar) {
			htable.fieldsTerminatedBy(tableSpecfication.fieldsTerminatedBy);
		}

		if (tableSpecfication.linesTerminatedBy != defaultChar) {
			htable.linesTerminatedBy(tableSpecfication.linesTerminatedBy);
		}
		
		if(tableSpecfication.columns != null){
			List<HCatFieldSchema> hiveColumn = ColumnMetaDataUtil.prepareHiveColumn(tableSpecfication.columns);
			htable.cols(hiveColumn);
		}
		
		if(tableSpecfication.partitionColumns != null){
			List<HCatFieldSchema> partitionColumns = ColumnMetaDataUtil.prepareHiveColumn(tableSpecfication.partitionColumns);
			htable.partCols(partitionColumns);
		}
		
		HCatCreateTableDesc.Builder tableDescBuilder = HCatCreateTableDesc
				.create(htable).ifNotExists(true);
		tableDescriptor = tableDescBuilder.build();

		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			client.createTable(tableDescriptor);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
	
	/**
	 * This method check table is created in hive metastroe
	 * @param databaseName
	 * @param tableName
	 * @return true if table is created, otherwise return false
	 * @throws HCatException
	 */
	public boolean isTableCreated(String databaseName, String tableName) throws HCatException{
		HCatClient client = null;
		boolean tableCreated = false;
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			HCatTable hcatTable = client.getTable(databaseName, tableName);
			Assert.hasText(hcatTable.getTableName(), "table is null");
			tableCreated = true;
		}finally{
			HiveClientProvider.closeClient(client);
		}		
		return tableCreated;
	}
	
	/**
	 * this method ensure the table is deleted from hive metastore, by default the isExist flag is true.
	 * @param tableSpecfication
	 * @throws HCatException
	 */
	public void dropTable(String databaseName,String tableName) throws HCatException{
		HCatClient client = null;
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			client.dropTable(databaseName, tableName, Boolean.TRUE);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
	/**
	 * 
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public TableMetaData getTableMetaData(String databaseName,String tableName) throws HCatException{
		HCatClient client = null;
		TableMetaData tableMetaData;
		HCatTable hcatUserTable = null;
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			hcatUserTable = client.getTable(databaseName, tableName);
			tableMetaData = new TableMetaData(hcatUserTable);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
		return tableMetaData;
	}
	
	
	public synchronized ReaderContext readData(String databaseName,
			String tableName, String filterCol,
			String host, int port, Map<String, String> configuration)
			throws HCatException {
//		HiveConf hcatConf;
//		hcatConf = new HiveConf(HiveTableManger.class);
		ReadEntity.Builder builder = new ReadEntity.Builder();
		
		ReadEntity entity = builder.withDatabase(databaseName)
				.withTable(tableName).withFilter(filterCol).build();
//		Map<String, String> configuration = new HashMap<String, String>();
	
		configuration.put(HiveConf.ConfVars.METASTOREURIS.toString(),
				"thrift://" + host +  ":"+ port);
//		configuration.put(
//				HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES.toString(),
//				"3");
////		configuration.put(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
////				hcatConf.get(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname));
////		configuration.put(HiveConf.ConfVars.PREEXECHOOKS.varname,
////				hcatConf.get(HiveConf.ConfVars.PREEXECHOOKS.varname));
////		configuration.put(HiveConf.ConfVars.POSTEXECHOOKS.varname,
////				hcatConf.get(HiveConf.ConfVars.POSTEXECHOOKS.varname));
//		configuration.put(
//				HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname,
//				"60");
//
//		configuration
//				.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
//						hcatConf.get(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname));

//		if (haEnabled) {
//			addDFSConfigurations(configuration);
//		}

		HCatReader reader = DataTransferFactory.getHCatReader(entity,
				configuration);
		
		ReaderContext context = null;
		try {

			context = reader.prepareRead();

		} catch (Throwable ex) {
			throw ex;
		}

		return context;
	}
//	public void addDFSConfigurations(Map<String, String> configuration) {
//
//		configuration.put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER.replace(HiveClientConstants.HA_SERVICE_NAME,
//				"hdfs-cluster"), "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//		configuration.put(HiveClientConstants.DFS_NAME_SERVICES, "hdfs-cluster");
//		configuration.put(HiveClientConstants.DFS_HA_NAMENODES.replace(HiveClientConstants.HA_SERVICE_NAME, "hdfs-cluster"),
//				HiveClientConstants.DFS_NAME_NODE_LIST);
//		configuration.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1.replace(
//				HiveClientConstants.HA_SERVICE_NAME, "hdfs-cluster"), "slcd000hnn201.stubcorp.com:8020");
//		configuration.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2.replace(
//				HiveClientConstants.HA_SERVICE_NAME, "hdfs-cluster"), "slcd000hnn203.stubcorp.com:8020");
//	}
}
