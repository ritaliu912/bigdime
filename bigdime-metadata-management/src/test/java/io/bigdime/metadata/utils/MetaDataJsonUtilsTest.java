/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata.utils;

import static org.mockito.MockitoAnnotations.initMocks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.core.commons.JsonHelper;

public class MetaDataJsonUtilsTest {
	@Mock
	MetadataStore metadataStore;

	JsonHelper jsonHelper;

	@BeforeTest
	public void setUp() {
		initMocks(this);
		jsonHelper = new JsonHelper();
	}

	String trackingSchema = "{ \"name\": \"MetaInformation\", \"version\": \"1.1.0\", \"type\": \"map\", \"entityProperties\": { \"hiveDatabase\": { \"name\": \"clickstream\", \"location\": \"/data/clickstream\" }, \"hiveTable\": { \"name\": \"clickStreamEvents\", \"type\": \"external\", \"location\": \"/data/clickstream/raw\" }, \"hivePartitions\": { \"feed\": { \"name\": \"feed\", \"type\": \"string\", \"comments\": \"The account or feed for a data stream.\" }, \"dt\": { \"name\": \"dt\", \"type\": \"string\", \"comments\": \"The date partition for a data stream.\" } }, \"properties\": { \"account\": { \"name\": \"account\", \"type\": \"string\", \"comments\": \"The identifier for the data feed.\" }, \"prop1\": { \"name\": \"prop1\", \"type\": \"string\", \"comments\": \"The identifier for the prop1\" }, \"prop2\": { \"name\": \"prop2\", \"type\": \"string\", \"comments\": \"The identifier for the prop2\" }, \"context1\": { \"name\": \"context1\", \"type\": \"string\", \"comments\": \"The identifier for the context1\" }, \"context2\": { \"name\": \"context2\", \"type\": \"string\", \"comments\": \"The identifier for the context2\" } } } }";

	@Test
	public void testConvertJsonToMetaData() throws JsonProcessingException, IOException {
		String entityName = "clickStreamEvents";
		MetaDataJsonUtils metadataJsonUtils = new MetaDataJsonUtils();
		ObjectMapper schemaMapper = new ObjectMapper();
		Metasegment metaSegment = metadataJsonUtils.convertJsonToMetaData("mock-app",entityName,
				schemaMapper.readTree(trackingSchema.getBytes()));
		JsonNode schema = schemaMapper.readTree(trackingSchema.getBytes());

		JsonNode entityProperties = jsonHelper.getRequiredNode(schema, "entityProperties");
		JsonNode hiveDatabase = jsonHelper.getRequiredNode(entityProperties, "hiveDatabase");
		JsonNode hivePartitions = jsonHelper.getRequiredNode(entityProperties, "hivePartitions");

		Assert.assertEquals(metaSegment.getAdaptorName(), "mock-app");
		Assert.assertEquals(metaSegment.getEntitees().size(), 1);
		Assert.assertEquals(
				metaSegment.getEntity(entityName).getAttributes().size(),
				5);
		Assert.assertEquals(metaSegment.getDatabaseName(), jsonHelper.getRequiredStringProperty(hiveDatabase, "name"));
		Assert.assertEquals(metaSegment.getDatabaseLocation(),
				jsonHelper.getRequiredStringProperty(hiveDatabase, "location"));
	}

	// @Test
	// public void testMapJsonToAttributeSet() throws JsonProcessingException,
	// IOException, MetadataAccessException {
	// try (InputStream is =
	// this.getClass().getClassLoader().getResourceAsStream("tracking.json")) {
	//
	// }
	//
	// }
	@Test(expectedExceptions = FileNotFoundException.class)
	public void testReadSchemaAndConvertToMetasegmentWithFileNotFound()
			throws JsonProcessingException, IOException, MetadataAccessException {
		MetaDataJsonUtils metadataJsonUtils = new MetaDataJsonUtils();
		List<Metasegment> metasegments = null;

		Mockito.when(metadataStore.getAdaptorMetasegments(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(metasegments);

		metadataJsonUtils.readSchemaAndConvertToMetasegment(metadataStore, "unit-adaptorName", "HIVE",
				"tracking-nofile.json");
	}

	@Test
	public void testReadSchemaAndConvertToMetasegment()
			throws JsonProcessingException, IOException, MetadataAccessException {
		MetaDataJsonUtils metadataJsonUtils = new MetaDataJsonUtils();
		List<Metasegment> metasegments = null;

		// List<Metasegment> dbMetasegments = new ArrayList<>();
		// Metasegment dbMetasegment = new Metasegment();

		Mockito.when(metadataStore.getAdaptorMetasegments(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(metasegments);

		// //(MetadataStore metadataStore,
		//// String adaptorName, String schemaType, String schemaFileName)
		// Metasegment metaSegment =
		List<Map<String, Metasegment>> twoItemList = metadataJsonUtils.readSchemaAndConvertToMetasegment(metadataStore,
				"unit-adaptorName", "HIVE", "tracking_one_entity.json");

		Assert.assertNotNull(twoItemList);
		Map<String, Metasegment> entityNameToMetasegmentMap = twoItemList.get(0);
		Assert.assertNotNull(entityNameToMetasegmentMap);
		Assert.assertEquals(entityNameToMetasegmentMap.size(), 1);

		Metasegment usMetasegment = twoItemList.get(0).get("unit_1_users");
		// Metasegment auMetasegment = twoItemList.get(0).get("unit_2_users");
		Assert.assertNotNull(usMetasegment);
		// Assert.assertNotNull(auMetasegment);

		Set<Entitee> entities = usMetasegment.getEntitees();
		Assert.assertNotNull(entities);
		Assert.assertEquals(entities.size(), 1);
		Set<Attribute> attributes = entities.iterator().next().getAttributes();
		Assert.assertNotNull(attributes);
		Assert.assertEquals(attributes.size(), 3);
		Iterator<Attribute> attributeIter = attributes.iterator();

		Assert.assertEquals(attributeIter.next().getAttributeName(), "daid_1");
		Assert.assertEquals(attributeIter.next().getAttributeName(), "prty_src_id");
		Assert.assertEquals(attributeIter.next().getAttributeName(), "indiv_id_2");
	}

	@Test
	public void testReadSchemaAndConvertToMetasegmentWithDbMetasegments()
			throws JsonProcessingException, IOException, MetadataAccessException {
		MetaDataJsonUtils metadataJsonUtils = new MetaDataJsonUtils();

		List<Metasegment> dbMetasegments = new ArrayList<>();

		Metasegment dbUsMetasegment = new Metasegment();
		Entitee usEntity = createNewEntity("unit_1_users", new String[] { "col1", "col2" });
		Set<Entitee> usEntities = createNewEntities(usEntity);
		dbUsMetasegment.setEntitees(usEntities);
		dbMetasegments.add(dbUsMetasegment);

		Metasegment dbAuMetasegment = new Metasegment();
		Entitee auEntity = createNewEntity("unit_2_users", new String[] { "col1" });
		Set<Entitee> auEntities = createNewEntities(auEntity);
		dbAuMetasegment.setEntitees(auEntities);
		dbMetasegments.add(dbAuMetasegment);

		Mockito.when(metadataStore.getAdaptorMetasegments(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(dbMetasegments);

		List<Map<String, Metasegment>> twoItemList = metadataJsonUtils.readSchemaAndConvertToMetasegment(metadataStore,
				"unit-adaptorName", "HIVE", "tracking.json");

		Assert.assertNotNull(twoItemList);
		Map<String, Metasegment> entityNameToMetasegmentMap = twoItemList.get(0);
		Assert.assertNotNull(entityNameToMetasegmentMap);
		Assert.assertEquals(entityNameToMetasegmentMap.size(), 2);

		// System.out.println(twoItemList.get(0).entrySet());
		System.out.println(twoItemList.get(1).entrySet());

		Metasegment usMetasegment = twoItemList.get(0).get("unit_1_users");
		Metasegment auMetasegment = twoItemList.get(1).get("unit_2_users");
		Assert.assertNotNull(usMetasegment);
		Assert.assertNotNull(auMetasegment);
		// Assert.assertNotNull(auMetasegment);

		Set<Entitee> entities = usMetasegment.getEntitees();
		Assert.assertNotNull(entities);
		Assert.assertEquals(entities.size(), 1);
		Set<Attribute> attributes = entities.iterator().next().getAttributes();
		Assert.assertNotNull(attributes);
		Assert.assertEquals(attributes.size(), 3);
	}

	@Test
	public void testPopulateMap() {
		Metasegment metasegment = new Metasegment();
		Entitee usEntity = createNewEntity("unit_1_users",
				new String[] { "col1", "col2:1", "col5", "col4", "col7", "col8", "col6" });
		Set<Entitee> usEntities = createNewEntities(usEntity);
		metasegment.setEntitees(usEntities);
		String[] values = { "val1", "val2", "val8", "val4", "val5", "val6", "val3" };
		Map<String, String> attributeMap = MetaDataJsonUtils.createAttributeNameValueMap(metasegment, "unit_1_users",
				values);

		Assert.assertEquals(attributeMap.get("col1"), "val1");
		Assert.assertEquals(attributeMap.get("col2:1"), "val2");
		Assert.assertEquals(attributeMap.get("col5"), "val8");
		Assert.assertEquals(attributeMap.get("col4"), "val4");
		Assert.assertEquals(attributeMap.get("col7"), "val5");
		Assert.assertEquals(attributeMap.get("col8"), "val6");
		Assert.assertEquals(attributeMap.get("col6"), "val3");
	}

	private Set<Entitee> createNewEntities(Entitee... entities) {
		Set<Entitee> schemaEntities = new HashSet<>();
		for (Entitee entity : entities)
			schemaEntities.add(entity);
		return schemaEntities;
	}

	// private Set<Entitee> createNewEntities(String entityName, String[]
	// attributeNames) {
	// Set<Entitee> schemaEntities = new HashSet<>();
	// Entitee schemaEntity = new Entitee();
	// schemaEntity.setEntityName(entityName);
	// Set<Attribute> schemaAttributes = createNewAttributes(attributeNames);
	// schemaEntity.setAttributes(schemaAttributes);
	// schemaEntities.add(schemaEntity);
	// return schemaEntities;
	// }

	private Entitee createNewEntity(String entityName, String[] attributeNames) {
		Entitee schemaEntity = new Entitee();
		schemaEntity.setEntityName(entityName);
		Set<Attribute> schemaAttributes = createNewAttributes(attributeNames);
		schemaEntity.setAttributes(schemaAttributes);
		return schemaEntity;
	}

	private Set<Attribute> createNewAttributes(String[] attributeNames) {
		Set<Attribute> attributes = new LinkedHashSet<>();
		for (String attributeName : attributeNames) {
			Attribute attribute = new Attribute();
			attribute.setAttributeName(attributeName);
			attributes.add(attribute);

		}
		return attributes;
	}

}
