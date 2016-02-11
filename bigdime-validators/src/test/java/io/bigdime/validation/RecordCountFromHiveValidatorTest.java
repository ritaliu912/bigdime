/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.DataValidationConstants;
import io.bigdime.validation.RecordCountFromHiveValidator;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.libs.hive.table.HiveTableManger;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.http.client.ClientProtocolException;
import org.testng.Assert;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@PrepareForTest(HiveTableManger.class)
public class RecordCountFromHiveValidatorTest extends PowerMockTestCase {

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveHostTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("");
		recordCountFromHiveValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHivePortTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn(null);
		recordCountFromHiveValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = NumberFormatException.class)
	public void validatePortNumberFormatTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("port");
		recordCountFromHiveValidator.validate(mockActionEvent);
	}
	
	@Test
	public void validateNotReadyTest() throws DataValidationException{
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.FALSE.toString());
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		Assert.assertEquals(recordCountFromHiveValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.NOT_READY);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullSrcRecordCountTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		recordCountFromHiveValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = NumberFormatException.class)
	public void validateSrcRCParseToIntTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "src");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		recordCountFromHiveValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveDBNameTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		recordCountFromHiveValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveTableNameTest() throws DataValidationException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		recordCountFromHiveValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void validateHiveTableNotCreated() throws DataValidationException, HCatException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "table");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(false);
		recordCountFromHiveValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void validateGetHDFSRecordCountException() throws DataValidationException, HCatException{
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "table");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.doThrow(HCatException.class).when(mockHiveTableManager).isTableCreated(anyString(), anyString());	
		recordCountFromHiveValidator.validate(mockActionEvent);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithoutPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "table");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
		mockMap.put(HiveClientConstants.HA_ENABLED, Boolean.FALSE.toString());
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyString(), anyInt(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(recordCountFromHiveValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithPartitionMatchTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "0");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "table");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "dt");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "20120201");
		mockMap.put(HiveClientConstants.HA_ENABLED, Boolean.FALSE.toString());
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyString(), anyInt(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(recordCountFromHiveValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.PASSED);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithHAEnabledNoPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "table");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
		mockMap.put(HiveClientConstants.HA_ENABLED, Boolean.TRUE.toString());
		mockMap.put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER, "dfs proxy");
		mockMap.put(HiveClientConstants.HA_SERVICE_NAME, "haServiceName");
		mockMap.put(HiveClientConstants.DFS_NAME_SERVICES, "dfs service");
		mockMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1, "dfs namenode 1");
		mockMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2, "dfs namenode 2");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyString(), anyInt(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(recordCountFromHiveValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test
	public void gettersAndSettersTest() {
		RecordCountFromHiveValidator recordCountFromHiveValidator = new RecordCountFromHiveValidator();
		recordCountFromHiveValidator.setName("testName");
		Assert.assertEquals(recordCountFromHiveValidator.getName(), "testName");
	}

}
