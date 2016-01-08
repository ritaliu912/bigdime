package io.bigdime.validation;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.table.HiveTableManger;

import org.apache.hive.hcatalog.common.HCatException;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

@PrepareForTest(HiveTableManger.class)
public class ColumnTypeValidatorTest extends PowerMockTestCase {
	
	@Test(priority = 1, expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveHostTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("");
		columnTypeValidator.validate(mockActionEvent);
	}

	@Test(priority = 2, expectedExceptions = IllegalArgumentException.class)
	public void validateNullHivePortNumberTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("hiveHost").thenReturn(null);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(priority = 3, expectedExceptions = NumberFormatException.class)
	public void validatePortNumberFormatTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "port");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(priority = 4, expectedExceptions = IllegalArgumentException.class)
	public void validateNullDBNameTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(priority = 5, expectedExceptions = IllegalArgumentException.class)
	public void validateNullEntityNameTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(priority = 6)
	public void validateHiveTableNotCreated() throws DataValidationException, HCatException{
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Properties props = Mockito.mock(Properties.class);
		HiveTableManger hiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance(props)).thenReturn(hiveTableManager);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "mockTable");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		ReflectionTestUtils.setField(columnTypeValidator, "props", props);
		ReflectionTestUtils.setField(columnTypeValidator, "hiveTableManager", hiveTableManager);
		Mockito.when(hiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(false);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);
	}
	
	@Test(priority = 7)
	public void validateEntityNotFound() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Properties props = Mockito.mock(Properties.class);
		HiveTableManger hiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance(props)).thenReturn(hiveTableManager);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "mockTable");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		
		ReflectionTestUtils.setField(columnTypeValidator, "props", props);
		ReflectionTestUtils.setField(columnTypeValidator, "hiveTableManager", hiveTableManager);
		Mockito.when(hiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(hiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		List<Column> hiveColumns = new ArrayList<Column>();
		Mockito.when(mockTable.getColumns()).thenReturn(hiveColumns);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(null);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);
	}
	
	@SuppressWarnings("unchecked")
	@Test(priority = 8)
	public void validateColumnTypeMatchWithSameColumnCount() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Properties props = Mockito.mock(Properties.class);
		HiveTableManger hiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance(props)).thenReturn(hiveTableManager);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "mockTable");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		
		ReflectionTestUtils.setField(columnTypeValidator, "props", props);
		ReflectionTestUtils.setField(columnTypeValidator, "hiveTableManager", hiveTableManager);
		Mockito.when(hiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(hiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		List<Column> hiveColumns = new ArrayList<Column>();
		Column column = new Column("id", "int", "comment");
		hiveColumns.add(column);
		List<Column> partitionColumns = new ArrayList<Column>();
		Column parColumn = new Column("dt", "string", "partition column");
		partitionColumns.add(parColumn);
		hiveColumns.addAll(partitionColumns);
		Mockito.when(mockTable.getColumns()).thenReturn(hiveColumns);
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		Metasegment mockMetasegment = Mockito.mock(Metasegment.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metasegment", mockMetasegment);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(mockMetasegment);
		Set<Entitee> mockEntitySet = Mockito.mock(Set.class);
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntitySet);
		Mockito.when(mockEntitySet.size()).thenReturn(Integer.valueOf(1));
		Entitee mockEntitee = Mockito.mock(Entitee.class);
		ReflectionTestUtils.setField(columnTypeValidator, "entitee", mockEntitee);
		Mockito.when(mockMetasegment.getEntity(anyString())).thenReturn(mockEntitee);
		Set<Attribute> metaColumns = new LinkedHashSet<Attribute>();
		Attribute attr1 = new Attribute();
		attr1.setAttributeName("id");
		attr1.setAttributeType("int");
		metaColumns.add(attr1);
		Attribute attr2 = new Attribute();
		attr2.setAttributeName("dt");
		attr2.setAttributeType("string");
		metaColumns.add(attr2);
		Mockito.when(mockEntitee.getAttributes()).thenReturn(metaColumns);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.PASSED);
	}
	
	@SuppressWarnings("unchecked")
	@Test(priority = 9)
	public void validateColumnTypeMismatchSourceColumnMore() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Properties props = Mockito.mock(Properties.class);
		HiveTableManger hiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance(props)).thenReturn(hiveTableManager);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "mockTable");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		
		ReflectionTestUtils.setField(columnTypeValidator, "props", props);
		ReflectionTestUtils.setField(columnTypeValidator, "hiveTableManager", hiveTableManager);
		Mockito.when(hiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(hiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		List<Column> hiveColumns = new ArrayList<Column>();
		Column column = new Column("id", "int", "comment");
		hiveColumns.add(column);
		List<Column> partitionColumns = new ArrayList<Column>();
		Column parColumn = new Column("dt", "string", "partition column");
		partitionColumns.add(parColumn);
		hiveColumns.addAll(partitionColumns);
		Mockito.when(mockTable.getColumns()).thenReturn(hiveColumns);
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		Metasegment mockMetasegment = Mockito.mock(Metasegment.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metasegment", mockMetasegment);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(mockMetasegment);
		Set<Entitee> mockEntitySet = Mockito.mock(Set.class);
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntitySet);
		Mockito.when(mockEntitySet.size()).thenReturn(Integer.valueOf(1));
		Entitee mockEntitee = Mockito.mock(Entitee.class);
		ReflectionTestUtils.setField(columnTypeValidator, "entitee", mockEntitee);
		Mockito.when(mockMetasegment.getEntity(anyString())).thenReturn(mockEntitee);
		Set<Attribute> metaColumns = new LinkedHashSet<Attribute>();
		Attribute attr1 = new Attribute();
		attr1.setAttributeName("id");
		attr1.setAttributeType("int");
		metaColumns.add(attr1);
		Attribute attr2 = new Attribute();
		attr2.setAttributeName("dt");
		attr2.setAttributeType("string");
		metaColumns.add(attr2);
		Attribute attr3 = new Attribute();
		attr3.setAttributeName("name");
		attr3.setAttributeType("string");
		metaColumns.add(attr3);
		Mockito.when(mockEntitee.getAttributes()).thenReturn(metaColumns);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.COLUMN_TYPE_MISMATCH);
	}
	
	@SuppressWarnings("unchecked")
	@Test(priority = 9)
	public void validateColumnTypeMismatchHiveColumnMore() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Properties props = Mockito.mock(Properties.class);
		HiveTableManger hiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance(props)).thenReturn(hiveTableManager);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "mockTable");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		
		ReflectionTestUtils.setField(columnTypeValidator, "props", props);
		ReflectionTestUtils.setField(columnTypeValidator, "hiveTableManager", hiveTableManager);
		Mockito.when(hiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(hiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		List<Column> hiveColumns = new ArrayList<Column>();
		Column column = new Column("id", "int", "comment");
		hiveColumns.add(column);
		column = new Column("first_name", "varchar", "comment");
		hiveColumns.add(column);
		column = new Column("last_name", "varchar", "comment");
		hiveColumns.add(column);
		List<Column> partitionColumns = new ArrayList<Column>();
		Column parColumn = new Column("dt", "string", "partition column");
		partitionColumns.add(parColumn);
		hiveColumns.addAll(partitionColumns);
		Mockito.when(mockTable.getColumns()).thenReturn(hiveColumns);
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		Metasegment mockMetasegment = Mockito.mock(Metasegment.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metasegment", mockMetasegment);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(mockMetasegment);
		Set<Entitee> mockEntitySet = Mockito.mock(Set.class);
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntitySet);
		Mockito.when(mockEntitySet.size()).thenReturn(Integer.valueOf(1));
		Entitee mockEntitee = Mockito.mock(Entitee.class);
		ReflectionTestUtils.setField(columnTypeValidator, "entitee", mockEntitee);
		Mockito.when(mockMetasegment.getEntity(anyString())).thenReturn(mockEntitee);
		Set<Attribute> metaColumns = new LinkedHashSet<Attribute>();
		Attribute attr1 = new Attribute();
		attr1.setAttributeName("id");
		attr1.setAttributeType("int");
		metaColumns.add(attr1);
		Attribute attr2 = new Attribute();
		attr2.setAttributeName("dt");
		attr2.setAttributeType("string");
		metaColumns.add(attr2);
		Mockito.when(mockEntitee.getAttributes()).thenReturn(metaColumns);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.COLUMN_TYPE_MISMATCH);
	}
	
	@Test(priority = 10)
	public void gettersAndSettersTest() {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		columnTypeValidator.setName("testName");
		Assert.assertEquals(columnTypeValidator.getName(), "testName");
	}
}
