/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hive;

import static io.bigdime.core.commons.DataConstants.CTRL_A;
import static io.bigdime.core.commons.DataConstants.EOL;
import static io.bigdime.core.commons.DataConstants.COLUMN_SEPARATED_BY;
import static io.bigdime.core.commons.DataConstants.ROW_SEPARATED_BY;
import static io.bigdime.core.commons.DataConstants.SCHEMA_FILE_NAME;
import static io.bigdime.core.commons.DataConstants.ENTITY_NAME;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.AdaptorContext;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.MapDescriptorParser;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.SimpleJournal;
@ContextConfiguration(classes = { MetaDataJsonUtils.class})
public class JsonHiveSchemaMapperHandlerTest extends AbstractTestNGSpringContextTests{
	@Mock
	MetadataStore metadataStore;
	JsonHelper jsonHelper;
	AdaptorConfig adaptorConfig;
	AdaptorContext adaptorcontext;
	Map<String, Object> propertyMap = null;
	@Autowired
	MetaDataJsonUtils metaDataJsonUtils;
	public static final String entityName = "clickStreamEvents";
	@BeforeTest
	public void setUp() throws JsonProcessingException, IOException {
		initMocks(this);
		adaptorConfig = AdaptorConfig.getInstance();
		adaptorcontext = adaptorConfig.getAdaptorContext();
		jsonHelper = new JsonHelper();
		propertyMap = new HashMap<String, Object>(); 
		propertyMap.put(ENTITY_NAME,entityName);
		propertyMap.put(SCHEMA_FILE_NAME,"click-stream-hive-schema.json");
		propertyMap.put(ROW_SEPARATED_BY,CTRL_A);
		propertyMap.put(COLUMN_SEPARATED_BY,EOL);
		
		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"clickStreamEvents\",\"topic\" : \"topic1\",\"partition\" : \"1\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));
		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		
	}

	String sampleMessage = "{ \"account\": \"desktop-us\", \"events\": [ { \"name\": \"banner:manualImpression\", \"properties\": { \"prop1\": \"mock1_value\", \"prop2\": \"mock2_value\" }, \"timestamp\": \"2014-09-23T15:15:30Z\" } ], \"context\": { \"context1\": \"c_mock1_value\", \"context2\": \"c_mock2_value\", \"context3\": \"c_mock2_value\" } }";
	String trackingSchema = "{ \"name\": \"MetaInformation\", \"version\": \"1.1.0\", \"type\": \"map\", \"entityProperties\": { \"hiveDatabase\": { \"name\": \"clickstream\", \"location\": \"/data/clickstream\" }, \"hiveTable\": { \"name\": \"clickStreamEvents\", \"type\": \"external\", \"location\": \"/data/clickstream/raw\" }, \"hivePartitions\": { \"feed\": { \"name\": \"feed\", \"type\": \"string\", \"comments\": \"The account or feed for a data stream.\" }, \"dt\": { \"name\": \"dt\", \"type\": \"string\", \"comments\": \"The date partition for a data stream.\" } }, \"properties\": { \"account\": { \"name\": \"account\", \"type\": \"string\", \"comments\": \"The identifier for the data feed.\" }, \"prop1\": { \"name\": \"prop1\", \"type\": \"string\", \"comments\": \"The identifier for the prop1\" }, \"prop2\": { \"name\": \"prop2\", \"type\": \"string\", \"comments\": \"The identifier for the prop2\" }, \"context1\": { \"name\": \"context1\", \"type\": \"string\", \"comments\": \"The identifier for the context1\" }, \"context2\": { \"name\": \"context2\", \"type\": \"string\", \"comments\": \"The identifier for the context2\" } } } }";
	String serverTimeStampMessage = "{ \"account\": \"desktop-us\", \"events\": [ { \"name\": \"banner:manualImpression\", \"properties\": { \"prop1\": \"mock1_value\", \"prop2\": \"mock2_value\" }, \"timestamp\": \"2014-09-23T15:15:30Z\" } ], \"context\": { \"context1\": \"c_mock1_value\", \"context2\": \"c_mock2_value\", \"context3\": \"c_mock2_value\", \"serverTimestamp\": \"2015-07-30T02:50:26Z\" } }";

	
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testBuild() throws AdaptorConfigurationException, MetadataAccessException {
		JsonProcessingException jsonProcessingException = mock(JsonProcessingException.class);
		Metasegment metaSegment = mock(Metasegment.class);
		when(metadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(null);

		JsonHiveSchemaMapperHandler hiveSchemaHandler = new JsonHiveSchemaMapperHandler();
		ReflectionTestUtils.setField(hiveSchemaHandler, "metadataStore", metadataStore);
		ReflectionTestUtils.setField(hiveSchemaHandler, "metaDataJsonUtils", metaDataJsonUtils);

		hiveSchemaHandler.setPropertyMap(propertyMap);
		hiveSchemaHandler.build();
		doThrow(new MetadataAccessException("errorMessage")).when(metadataStore).put(any(Metasegment.class));
		hiveSchemaHandler.build();
		when(metadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(metaSegment);
		hiveSchemaHandler.build();
		doThrow(jsonProcessingException).when(metadataStore).put(any(Metasegment.class));
		hiveSchemaHandler.build();

	}
	/**
	 * code coverage test
	 * @throws IOException
	 * @throws MetadataAccessException
	 */
	@Test
	public void testVerifyAndRetryRecordsDiscrepency() throws IOException, MetadataAccessException{
		JsonHiveSchemaMapperHandler hiveSchemaHandler = new JsonHiveSchemaMapperHandler();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Metasegment metaSegment = mock(Metasegment.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> eventRecordMap = mock(HashMap.class);
		ReflectionTestUtils.setField(hiveSchemaHandler, "metadataStore", metadataStore);
		ReflectionTestUtils.setField(hiveSchemaHandler, "metaDataJsonUtils", metaDataJsonUtils);
		hiveSchemaHandler.verifyAndRetryRecordsDiscrepency(baos, metaSegment, eventRecordMap, 10, 15);
	}

	@Test
	public void testVerifyServerTimeStamp() throws JsonProcessingException, IOException, HandlerException, MetadataAccessException{
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);

		JsonHiveSchemaMapperHandler hiveSchemaHandler = new JsonHiveSchemaMapperHandler();
		ReflectionTestUtils.setField(hiveSchemaHandler, "entityName", entityName);
		ObjectMapper objectMapper1 = new ObjectMapper();
		Metasegment metaSegment = metaDataJsonUtils.convertJsonToMetaData("mock-app","clickStreamEvents",
				objectMapper1.readTree(trackingSchema.getBytes()));
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(serverTimeStampMessage.getBytes());
		ReflectionTestUtils.setField(hiveSchemaHandler, "jsonHelper", jsonHelper);
		ReflectionTestUtils.setField(hiveSchemaHandler, "metadataStore", mockMetadataStore);
		ReflectionTestUtils.setField(hiveSchemaHandler, "objectMapper", objectMapper1);
		ReflectionTestUtils.setField(hiveSchemaHandler, "columnSeparatedBy", CTRL_A);
		ReflectionTestUtils.setField(hiveSchemaHandler, "rowSeparatedBy", EOL);
		when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(metaSegment);
		hiveSchemaHandler.setPropertyMap(propertyMap);

		HandlerContext.get().createSingleItemEventList(actionEvent);
		hiveSchemaHandler.process();
	}
	@Test
	public void testProcess() throws HandlerException, MetadataAccessException, JsonProcessingException, IOException {
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		JsonHiveSchemaMapperHandler hiveSchemaHandler = new JsonHiveSchemaMapperHandler();
		hiveSchemaHandler.setPropertyMap(propertyMap);
		ReflectionTestUtils.setField(hiveSchemaHandler, "entityName", entityName);
		ObjectMapper objectMapper1 = new ObjectMapper();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(sampleMessage.getBytes());
		Metasegment metaSegment = metaDataJsonUtils.convertJsonToMetaData("mock-app","clickStreamEvents",
				objectMapper1.readTree(trackingSchema.getBytes()));
		adaptorcontext.setAdaptorName("mock-adapter");

		ReflectionTestUtils.setField(hiveSchemaHandler, "jsonHelper", jsonHelper);

		ReflectionTestUtils.setField(hiveSchemaHandler, "metadataStore", mockMetadataStore);
		ReflectionTestUtils.setField(hiveSchemaHandler, "objectMapper", objectMapper1);
		ReflectionTestUtils.setField(hiveSchemaHandler, "columnSeparatedBy", CTRL_A);
		ReflectionTestUtils.setField(hiveSchemaHandler, "rowSeparatedBy", EOL);

		@SuppressWarnings("rawtypes")
		Iterator it = mock(Iterator.class);
		when(it.hasNext()).thenReturn(Boolean.TRUE);
		when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(metaSegment);
		HandlerContext.get().createSingleItemEventList(actionEvent);
		hiveSchemaHandler.process();
	}

	@Test
	public void testProcessWithDataInJournal()
			throws HandlerException, MetadataAccessException, JsonProcessingException, IOException {
		JsonHiveSchemaMapperHandler hiveSchemaHandler = setHandler();
		ReflectionTestUtils.setField(hiveSchemaHandler, "entityName", entityName);
		SimpleJournal simpleJournal = new SimpleJournal();
		List<ActionEvent> eventList = new ArrayList<>();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(sampleMessage.getBytes());
		eventList.add(actionEvent);
		simpleJournal.setEventList(eventList);
		HandlerContext.get().setJournal(hiveSchemaHandler.getId(), simpleJournal);
		hiveSchemaHandler.process();
		Assert.assertNull(simpleJournal.getEventList());
	}

	@Test
	public void testProcessWithMoreThanOneEventInJournal()
			throws HandlerException, MetadataAccessException, JsonProcessingException, IOException {
		JsonHiveSchemaMapperHandler hiveSchemaHandler = setHandler();
		ReflectionTestUtils.setField(hiveSchemaHandler, "entityName", entityName);
		SimpleJournal simpleJournal = new SimpleJournal();
		List<ActionEvent> eventList = new ArrayList<>();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(sampleMessage.getBytes(Charset.defaultCharset()));
		eventList.add(actionEvent);
		actionEvent = new ActionEvent();
		actionEvent.setBody(sampleMessage.getBytes());
		eventList.add(actionEvent);
		simpleJournal.setEventList(eventList);
		HandlerContext.get().setJournal(hiveSchemaHandler.getId(), simpleJournal);
		hiveSchemaHandler.process();
		Assert.assertEquals(simpleJournal.getEventList().size(), 1);
	}

	@Test
	public void testProcessWithDataInJournalAndOutputChannel()
			throws HandlerException, MetadataAccessException, JsonProcessingException, IOException {
		JsonHiveSchemaMapperHandler hiveSchemaHandler = setHandler();
		DataChannel channel = Mockito.mock(DataChannel.class);
		ReflectionTestUtils.setField(hiveSchemaHandler, "outputChannel", channel);
		Mockito.doNothing().when(channel).put(Mockito.any(ActionEvent.class));
		ReflectionTestUtils.setField(hiveSchemaHandler, "entityName", entityName);

		SimpleJournal simpleJournal = new SimpleJournal();
		List<ActionEvent> eventList = new ArrayList<>();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(sampleMessage.getBytes(Charset.defaultCharset()));
		eventList.add(actionEvent);
		simpleJournal.setEventList(eventList);
		HandlerContext.get().setJournal(hiveSchemaHandler.getId(), simpleJournal);
		hiveSchemaHandler.process();
		Assert.assertNull(simpleJournal.getEventList());
	}

	private JsonHiveSchemaMapperHandler setHandler()
			throws JsonProcessingException, IOException, MetadataAccessException {
		JsonHiveSchemaMapperHandler hiveSchemaHandler = new JsonHiveSchemaMapperHandler();
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(hiveSchemaHandler, "metadataStore", mockMetadataStore);
		ObjectMapper objectMapper1 = new ObjectMapper();
		Metasegment metaSegment = metaDataJsonUtils.convertJsonToMetaData("mock-app","clickStreamEvents",
				objectMapper1.readTree(trackingSchema.getBytes(Charset.defaultCharset())));

		adaptorcontext.setAdaptorName("mock-adapter");

		ReflectionTestUtils.setField(hiveSchemaHandler, "jsonHelper", jsonHelper);
		ReflectionTestUtils.setField(hiveSchemaHandler, "objectMapper", objectMapper1);
		ReflectionTestUtils.setField(hiveSchemaHandler, "columnSeparatedBy", CTRL_A);
		ReflectionTestUtils.setField(hiveSchemaHandler, "rowSeparatedBy", EOL);

		@SuppressWarnings("rawtypes")
		Iterator it = mock(Iterator.class);
		when(it.hasNext()).thenReturn(Boolean.TRUE);
		when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(metaSegment);
		return hiveSchemaHandler;
	}

}
