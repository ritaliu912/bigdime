/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hive;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.common.testutils.factory.EmbeddedHiveServer;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.HandlerException;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.libs.hive.database.HiveDBManger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.http.client.ClientProtocolException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@ContextConfiguration(classes = { MetaDataJsonUtils.class})
public class HiveMetaDataHandlerTest extends AbstractTestNGSpringContextTests{
	@Mock
	MetadataStore metadataStore;
	@Autowired
	MetaDataJsonUtils metaDataJsonUtils;
	ObjectMapper objectMapper1 = new ObjectMapper();
	String trackingSchema = "{ \"name\": \"MetaInformation\", \"version\": \"1.1.0\", \"type\": \"map\", \"entityProperties\": { \"hiveDatabase\": { \"name\": \"clickstream\", \"location\": \"/tmp/data\", \"external\": \"true\" }, \"hiveTable\": { \"name\": \"clickStreamEvents\", \"type\": \"external\", \"location\": \"/data/clickstream/raw\" }, \"hivePartitions\": { \"feed\": { \"name\": \"feed\", \"type\": \"string\", \"comments\": \"The account or feed for a data stream.\" }, \"dt\": { \"name\": \"dt\", \"type\": \"string\", \"comments\": \"The date partition for a data stream.\" } }, \"properties\": { \"account\": { \"name\": \"account\", \"type\": \"string\", \"comments\": \"The identifier for the data feed.\" }, \"prop1\": { \"name\": \"prop1\", \"type\": \"string\", \"comments\": \"The identifier for the prop1\" }, \"prop2\": { \"name\": \"prop2\", \"type\": \"string\", \"comments\": \"The identifier for the prop2\" }, \"context1\": { \"name\": \"context1\", \"type\": \"string\", \"comments\": \"The identifier for the context1\" }, \"context2\": { \"name\": \"context2\", \"type\": \"string\", \"comments\": \"The identifier for the context2\" } } } }";
	EmbeddedHiveServer embeddedHiveServer = null;

	Properties props = new Properties();

	@BeforeClass
	public void before() throws InterruptedException, IOException{
		embeddedHiveServer = EmbeddedHiveServer.getInstance();
		embeddedHiveServer.startMetaStore();
		props.put(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + embeddedHiveServer.getHivePort());
		props.put(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
		props.put(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
		props.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, Boolean.FALSE.toString());
	}
	
	@Test
	public void testProcess() throws ClientProtocolException, IOException, InterruptedException, MetadataAccessException, HandlerException{
		cleanUp();
		HiveMetaDataHandler hiveMetaDataHandler = mockHiveMetaDataHandler();
		
		HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = new ArrayList<>();
		ActionEvent actionEvent = new ActionEvent();
		HashMap<String,String> headers = new HashMap<String,String>();
		headers.put(ActionEventHeaderConstants.ENTITY_NAME, "clickStreamEvents");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "account,dt");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "testaccount,20150101");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_LOCATION, FileUtils.getTempDirectoryPath()+File.separator
				+headers.get(ActionEventHeaderConstants.ENTITY_NAME)+File.separator+"20150101");
		
		actionEvent.setHeaders(headers);
		actionEvents.add(actionEvent);
		handlerContext.setEventList(actionEvents);
		hiveMetaDataHandler.process();
	}
	public void cleanUp() throws HCatException{
		HiveDBManger hiveDBManager = HiveDBManger.getInstance(props);
		hiveDBManager.dropDatabase("clickstream");
	}
	private HiveMetaDataHandler mockHiveMetaDataHandler()
			throws ClientProtocolException, IOException, InterruptedException, MetadataAccessException {
		HiveMetaDataHandler hiveMetaDataHandler = new HiveMetaDataHandler();
		metadataStore = Mockito.mock(MetadataStore.class);
		Metasegment metaSegment = metaDataJsonUtils.convertJsonToMetaData("mock-app","clickStreamEvents",
				objectMapper1.readTree(trackingSchema.getBytes()));
		when(metadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(metaSegment);
		when(metadataStore.getAdaptorEntity(anyString(), anyString(), anyString())).thenReturn(metaSegment.getEntity("clickStreamEvents"));
		metaSegment.setDatabaseLocation(FileUtils.getUserDirectoryPath() +File.separator+"BD_TEST"+"_"+System.currentTimeMillis()+metaSegment.getDatabaseName()+File.separator);
		//when(metaSegment.getDatabaseLocation()).thenReturn(FileUtils.getTempDirectoryPath()+File.separator+metaSegment.getDatabaseName()+File.separator);
		ReflectionTestUtils.setField(hiveMetaDataHandler, "metadataStore", metadataStore);
		ReflectionTestUtils.setField(hiveMetaDataHandler, "props", props);


		return hiveMetaDataHandler;

	}	
}
