/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.http.client.ClientProtocolException;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.validation.common.AbstractValidator;

/**
 * RecordCountValidator class is to perform whether hdfs record count from hive matches source record count or not
 * 
 * record count mismatches --- validation fail
 * otherwise, validation success
 * 
 * @author Rita Liu
 */

@Factory(id = "record_count_hive", type = RecordCountFromHiveValidator.class)
@Component
public class RecordCountFromHiveValidator implements Validator {

	private static final Logger logger = LoggerFactory.getLogger(RecordCountFromHiveValidator.class);
	
	private String name;

	protected boolean isReadyToValidate(final ActionEvent actionEvent) {
		String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hdfsFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_FILE_NAME);


		String totalSize = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_SIZE);
		String totalRead = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_READ);
		
		String validationReady = actionEvent.getHeaders().get(ActionEventHeaderConstants.VALIDATION_READY);
		if (validationReady == null || !validationReady.equalsIgnoreCase("true")) {
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"processing RecordCountValidator",
					"Record Count validation being skipped, totalSize={} totalRead={} hdfsBasePath={} hdfsFileName={} validationReady={}",
					totalSize, totalRead, hdfsBasePath, hdfsFileName, validationReady);
			return false;
		}
		return true;
	}
	
	/**
	 * This validate method will compare Hdfs record count(get from Hive) and source record
	 * count
	 * 
	 * @param actionEvent
	 * @return true if Hdfs record count is same as source record count,
	 *         otherwise return false
	 * 
	 */

	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {
		ValidationResponse validationPassed = new ValidationResponse();
		AbstractValidator commonCheckValidator = new AbstractValidator();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		int port = 0;
		String hiveHost = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_HOST_NAME);
		String portString = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PORT);
		String srcRCString = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_RECORD_COUNT);
		String hiveDBName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_DB_NAME);
		String hiveTableName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_TABLE_NAME);
		String hivePartitionNames = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES);
		String hivePartitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
		
		int sourceRecordCount = 0;

		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_HOST_NAME, hiveHost);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_PORT, portString);
		try {
			 port = Integer.parseInt(portString);
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal port number input{} while parsing string to integer", portString);
			throw new NumberFormatException();
		}

		if (!isReadyToValidate(actionEvent)) {
			validationPassed.setValidationResult(ValidationResult.NOT_READY);
			return validationPassed;
		}
		
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, srcRCString);
		try {
			sourceRecordCount = Integer.parseInt(srcRCString);
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal source record count input while parsing string to integer");
			throw new NumberFormatException();
		}
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_DB_NAME, hiveDBName);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_TABLE_NAME, hiveTableName);
		Map<String, String> partitionColumnsMap = new LinkedHashMap<String, String>();
		if (StringUtils.isNotBlank(hivePartitionNames) && StringUtils.isNotBlank(hivePartitionValues)) {
			String[] partitionNameList = hivePartitionNames.split(DataConstants.COMMA);
			String[] partitionValueList = hivePartitionValues.split(DataConstants.COMMA);
			for (int i = 0; i < partitionNameList.length; i++) {
				partitionColumnsMap.put(partitionNameList[i].trim(), partitionValueList[i].trim());
			}
		}
		int hdfsRecordCount = 0;
		Properties props = new Properties();
		props.put(HiveConf.ConfVars.METASTOREURIS, "thrift://" + hiveHost
				+ DataConstants.COLON + port);
		try {
			hdfsRecordCount = getHdfsRecordCount(props, hiveDBName, hiveTableName, partitionColumnsMap, 
								hiveHost, port, getHAProperties(actionEvent));
		} catch (HCatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
					.getAdaptorName(), "HCatException",
					"Exception occurred while getting record count from hive, cause: "
							+ e.getCause());
			throw new DataValidationException(
					"Exception during getting record count from hive");
		}

		if (sourceRecordCount == hdfsRecordCount) {
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Record count matches",
					"Hdfs record count({}) is same as source record count({}), hiveDBName: {}, hiveTableName: {}, partitionMap: {}", hdfsRecordCount, 
					sourceRecordCount, hiveDBName, hiveTableName, partitionColumnsMap);
			validationPassed.setValidationResult(ValidationResult.PASSED);
		} else {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Record count mismatches",
					"Hdfs record count({}) is not same as source record count({}), hiveDBName: {}, hiveTableName: {}, partitionMap: {}", hdfsRecordCount,
					sourceRecordCount, hiveDBName, hiveTableName, partitionColumnsMap);
			validationPassed.setValidationResult(ValidationResult.FAILED);
		}

		return validationPassed;
	}

	/**
	 * This is for hdfs record count using Hive
	 * 
	 * @param fileName
	 *            hdfs file name
	 * @throws DataValidationException 
	 * @throws IOException
	 * @throws ClientProtocolException
	 * 
	 */
	private int getHdfsRecordCount(Properties props, String databaseName, String tableName, Map<String, String> partitionMap,
						String host, int port, Map<String, String> configuration) throws HCatException, DataValidationException{
		int count = 0;
		String filterValue="";
		StringBuilder sb = new StringBuilder();
		if(partitionMap!=null){
			Set<String> partitionNames = partitionMap.keySet();
			Iterator<String> iter = partitionNames.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				if(!key.equalsIgnoreCase(ActionEventHeaderConstants.ENTITY_NAME)){
					filterValue = key
						+ "=\""
						+ partitionMap.get(key)
						+ "\"";
					sb.append(filterValue);
				}
			}
		}
		HiveTableManger hiveTableManager = HiveTableManger.getInstance(props);
		if(!hiveTableManager.isTableCreated(databaseName, tableName)){
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
					.getAdaptorName(), "Hive table not exist", 
					"Hive table {} is not found in {} database", tableName, databaseName);
			throw new DataValidationException("Hive table is not found!");
		}else{
			ReaderContext cntxt = hiveTableManager.readData(
				databaseName,
				tableName,
				sb.toString(),
				host, port,
				configuration);
			
			for (int slaveNode = 0; slaveNode < cntxt.numSplits(); slaveNode++) {
				HCatReader reader = DataTransferFactory.getHCatReader(cntxt,
					slaveNode);
				Iterator<HCatRecord> records = reader.read();
				while (records.hasNext()) {
					count++;
					records.next();
				}
			}
		}
		return count;	
	}
	
	private Map<String, String> getHAProperties(ActionEvent actionEvent) {
		Map<String, String> configMap = new HashMap<String, String>();
		String haEnabledString = actionEvent.getHeaders().get(HiveClientConstants.HA_ENABLED);
		Boolean haEnabled = Boolean.valueOf(haEnabledString);
		if(haEnabled){
			String hiveProxyProvider = actionEvent.getHeaders().get(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER);
			String haServiceName = actionEvent.getHeaders().get(HiveClientConstants.HA_SERVICE_NAME);
			String dfsNameService = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_SERVICES);
			String dfsNameNode1 = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1);
			String dfsNameNode2 = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2);
		
			configMap.put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER.replace(HiveClientConstants.HA_SERVICE_NAME,
				haServiceName), hiveProxyProvider);
			configMap.put(HiveClientConstants.DFS_NAME_SERVICES, dfsNameService);
			configMap.put(HiveClientConstants.DFS_HA_NAMENODES.replace(HiveClientConstants.HA_SERVICE_NAME, haServiceName),
					HiveClientConstants.DFS_NAME_NODE_LIST);
			configMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1.replace(
					HiveClientConstants.HA_SERVICE_NAME, haServiceName), dfsNameNode1);
			configMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2.replace(
					HiveClientConstants.HA_SERVICE_NAME, haServiceName), dfsNameNode2);
		}
		return configMap;
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
