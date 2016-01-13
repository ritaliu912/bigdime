/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.MapDescriptorParser;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.constants.KafkaReaderHandlerConstants;
import io.bigdime.libs.kafka.consumers.KafkaMessage;
import io.bigdime.libs.kafka.consumers.KafkaSimpleConsumer;
import io.bigdime.libs.kafka.exceptions.KafkaReaderException;

public class KafkaReaderHandlerTest {

	@Test
	public void testConstructor() {
		new KafkaReaderHandler();
	}

	@Test
	public void testBuild() throws HandlerException, AdaptorConfigurationException, JsonProcessingException, IOException {
		KafkaReaderHandler handler = new KafkaReaderHandler();
		Map<String, Object> propertyMap = new HashMap<>();
		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"topic1\",\"partition\" : \"1\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));


		
		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		propertyMap.put(KafkaReaderHandlerConstants.BROKERS, "unit-test-ip-1:9999,unit-test-ip-2:8888");
		propertyMap.put(KafkaReaderHandlerConstants.OFFSET_DATA_DIR, "/tmp");
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(handler, "runtimeInfoStore", runtimeInfoStore);
		handler.setPropertyMap(propertyMap);
		handler.build();
		Assert.assertEquals(handler.getInputDescriptor().getTopic(), "topic1");
		Assert.assertEquals(handler.getInputDescriptor().getPartition(), 1);
		Assert.assertEquals(handler.getInputDescriptor().getEntityName(), "unit-entity-name-value");
		
	}


	/**
	 * src-desc must be in topic:parition format and partition should be an
	 * integer value.
	 * 
	 * @throws HandlerException
	 * @throws AdaptorConfigurationException
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 */
	@Test(expectedExceptions = { AdaptorConfigurationException.class,
			InvalidValueConfigurationException.class }, expectedExceptionsMessageRegExp = "incorrect value specified in src-desc .*")
	public void testBuildWithInvalidPartition() throws HandlerException, AdaptorConfigurationException, JsonProcessingException, IOException {
		KafkaReaderHandler handler = new KafkaReaderHandler();
		Map<String, Object> propertyMap = new HashMap<>();

		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"topic1\",\"partition\" : \"test\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));

		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());

		handler.setPropertyMap(propertyMap);
		handler.build();
	}

	/**
	 * Test the process method, make sure that pollData is invoked. Run this
	 * test in a new thread to ensure that the HandlerContext is created from
	 * scratch.
	 * 
	 * @throws HandlerException
	 * @throws DataAdaptorException
	 * @throws KafkaReaderException
	 * @throws InterruptedException
	 */
	@Test
	public void testProcess() throws InterruptedException {
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
				KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
				List<KafkaMessage> kafkaMessages = new ArrayList<>();
				KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				long readOffset = 0l;
				Mockito.when(kafkaSimpleConsumer.getEarliestOffSet()).thenReturn(readOffset);
				Mockito.when(kafkaSimpleConsumer.pollData(readOffset)).thenReturn(kafkaMessages);
				
				ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
				KafkaInputDescriptor descriptor = new KafkaInputDescriptor("unit-topic", 1);
				ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
				// ReflectionTestUtils.setField(kafkaReaderHandler, "partition",
				// 1);
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				ReflectionTestUtils.setField(kafkaReaderHandler, "runtimeInfoStore", runtimeInfoStore);

				RuntimeInfo runtimeInfo = new RuntimeInfo();
				runtimeInfo.setProperties(new HashMap<String, String>());
				runtimeInfo.getProperties().put("kafka_message_offset", "0");
				Mockito.when(runtimeInfoStore.get(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
						.thenReturn(runtimeInfo);

				Status status = kafkaReaderHandler.process();
				Mockito.verify(kafkaSimpleConsumer, Mockito.times(1)).pollData(readOffset);
				Assert.assertSame(status, Status.CALLBACK);
				Assert.assertEquals(kafkaReaderHandler.getTotalInvocations(), 1);
				Assert.assertEquals(
						HandlerContext.get().getEventList().get(0).getHeaders()
								.get(ActionEventHeaderConstants.ENTITY_NAME),
						"unit-topic", "header with key=entityName must be set by KafkaReaderHandler");
				Assert.assertEquals(
						HandlerContext.get().getEventList().get(0).getHeaders()
								.get(ActionEventHeaderConstants.INPUT_DESCRIPTOR),
						"1", "header with key=inputDescriptor must be set by KafkaReaderHandler");
				return status;
			}
		});
		executorService.execute(futureTask);

		try {
			futureTask.get();
		} catch (ExecutionException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * Make sure that process method propaates the KafkaReaderException to the
	 * caller.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = KafkaReaderException.class)
	public void testProcessWithKafkaReaderException() throws Throwable {
		FutureTask<ActionEvent> futureTask = new FutureTask<>(new Callable<ActionEvent>() {
			@Override
			public ActionEvent call() throws Exception {
				long readOffset = 0l;
				KafkaInputDescriptor descriptor = new KafkaInputDescriptor();
				KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
				KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
				List<KafkaMessage> kafkaMessages = new ArrayList<>();
				KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				Mockito.when(kafkaSimpleConsumer.pollData(readOffset)).thenThrow(new KafkaReaderException("unit-test"));

				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				ReflectionTestUtils.setField(kafkaReaderHandler, "runtimeInfoStore", runtimeInfoStore);
				
				ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
				ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
				kafkaReaderHandler.process();
				Assert.fail("should have thrown KafkaReaderException");
				return null;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause().getCause();
		}
	}

	@Test
	public void testProcessWithNoMessagesFromConsumer() throws Throwable {
		FutureTask<ActionEvent> futureTask = new FutureTask<>(new Callable<ActionEvent>() {
			@Override
			public ActionEvent call() throws Exception {
				long readOffset = 0l;
				KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
				KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
				KafkaInputDescriptor descriptor = new KafkaInputDescriptor();
				List<KafkaMessage> kafkaMessages = new ArrayList<>();
				KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				Mockito.when(kafkaSimpleConsumer.pollData(readOffset)).thenReturn(null);
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				ReflectionTestUtils.setField(kafkaReaderHandler, "runtimeInfoStore", runtimeInfoStore);
				
				ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
				ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
				Status status = kafkaReaderHandler.process();
				Assert.assertSame(status, Status.BACKOFF);
				return null;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause().getCause();
		}
	}

	@Test
	public void testWithDataInJournal() throws HandlerException {
		KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
		HandlerJournal journal = new SimpleJournal();
		journal.setTotalRead(1);
		journal.setTotalSize(10);
		HandlerContext.get().setJournal(kafkaReaderHandler.getId(), journal);

		List<KafkaMessage> kafkaMessages = new ArrayList<>();
		kafkaMessages.add(Mockito.mock(KafkaMessage.class));
		kafkaMessages.add(Mockito.mock(KafkaMessage.class));
		ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaMessages", kafkaMessages);
		KafkaInputDescriptor descriptor = new KafkaInputDescriptor();
		ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
		Status status = kafkaReaderHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
	}

	@Test
	public void testWithDataInJournalWithChannel() throws HandlerException {
		KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
		HandlerJournal journal = new SimpleJournal();
		journal.setTotalRead(1);
		journal.setTotalSize(10);
		HandlerContext.get().setJournal(kafkaReaderHandler.getId(), journal);

		DataChannel channel = Mockito.mock(DataChannel.class);
		ReflectionTestUtils.setField(kafkaReaderHandler, "outputChannel", channel);

		Mockito.doNothing().when(channel).put(Mockito.any(ActionEvent.class));

		List<KafkaMessage> kafkaMessages = new ArrayList<>();
		kafkaMessages.add(Mockito.mock(KafkaMessage.class));
		kafkaMessages.add(Mockito.mock(KafkaMessage.class));
		ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaMessages", kafkaMessages);
		KafkaInputDescriptor descriptor = new KafkaInputDescriptor();
		ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
		Status status = kafkaReaderHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
	}

	/**
	 * If the RuntimeStore returns an offset that's bigger than size of
	 * collection returned by pollData, KafkaReaderHandler should BACKOFF.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testProcessWithLargeOffsetFromRuntimeInfo() throws Throwable {
		FutureTask<ActionEvent> futureTask = new FutureTask<>(new Callable<ActionEvent>() {
			@Override
			public ActionEvent call() throws Exception {
				long readOffset = 0l;
				KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
				KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
				List<KafkaMessage> kafkaMessages = new ArrayList<>();
				KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);
				Mockito.when(kafkaSimpleConsumer.pollData(readOffset)).thenReturn(kafkaMessages);
				ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
				KafkaInputDescriptor descriptor = new KafkaInputDescriptor("unit-topic", 1);
				ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				ReflectionTestUtils.setField(kafkaReaderHandler, "runtimeInfoStore", runtimeInfoStore);

				RuntimeInfo runtimeInfo = new RuntimeInfo();
				runtimeInfo.setProperties(new HashMap<String, String>());
				runtimeInfo.getProperties().put("kafka_message_offset", "100");
				Mockito.when(runtimeInfoStore.get(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
						.thenReturn(runtimeInfo);

				Status status = kafkaReaderHandler.process();
				Mockito.verify(kafkaSimpleConsumer, Mockito.times(1)).pollData(readOffset);
				Assert.assertSame(status, Status.BACKOFF);
				Assert.assertEquals(kafkaReaderHandler.getTotalInvocations(), 1);
				Assert.assertTrue(HandlerContext.get().getEventList().isEmpty(),
						"with backoff status, the handlerContext should not have any events");
				return null;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}
	/**
	 * If the RuntimeStore throws a RuntimeInfoStoreException,
	 * KafkaReaderHandler cant proceed.
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "io.bigdime.core.runtimeinfo.RuntimeInfoStoreException: *")
	public void tesBuildWithExceptionFromRuntimeInfo() throws AdaptorConfigurationException, RuntimeInfoStoreException, JsonProcessingException, IOException{
		KafkaReaderHandler handler = new KafkaReaderHandler();
		Map<String, Object> propertyMap = new HashMap<>();

		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"unit-topic-value\",\"partition\" : \"0\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));

		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());

		handler.setPropertyMap(propertyMap);
		@SuppressWarnings("unchecked")
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(handler, "runtimeInfoStore", runtimeInfoStore);

		Mockito.when(runtimeInfoStore.get(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
				.thenThrow(new RuntimeInfoStoreException(""));
		handler.build();
	}


	/**
	 * If the RuntimeStore returns null, assume that it does not have any data
	 * for this source, hence all the messages from the polldata need to be
	 * processed. Since the pollData returns 2 events, process method will
	 * process one and return status as callback to process remaining.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testProcessWithRuntimeInfoStoreReturningNull() throws Throwable {
		FutureTask<ActionEvent> futureTask = new FutureTask<>(new Callable<ActionEvent>() {
			@Override
			public ActionEvent call() throws Exception {
				long readOffset = 0l;
				KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
				KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
				List<KafkaMessage> kafkaMessages = new ArrayList<>();
				KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				Mockito.when(kafkaSimpleConsumer.pollData(readOffset)).thenReturn(kafkaMessages);
				ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
				KafkaInputDescriptor descriptor = new KafkaInputDescriptor("unit-topic", 1);
				ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				ReflectionTestUtils.setField(kafkaReaderHandler, "runtimeInfoStore", runtimeInfoStore);

				RuntimeInfo runtimeInfo = null;
				Mockito.when(runtimeInfoStore.get(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
						.thenReturn(runtimeInfo);

				Status status = kafkaReaderHandler.process();
				Mockito.verify(kafkaSimpleConsumer, Mockito.times(1)).pollData(readOffset);
				Assert.assertSame(status, Status.CALLBACK);
				Assert.assertEquals(kafkaReaderHandler.getTotalInvocations(), 1);
				Assert.assertEquals(HandlerContext.get().getEventList().size(), 1);
				return null;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	/**
	 * If the RuntimeStore throws a IllegalArgumentException, assume that it
	 * does not have any data for this source, hence all the messages from the
	 * polldata need to be processed. Since the pollData returns 2 events,
	 * process method will process one and return status as callback to process
	 * remaining.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testProcessWithRuntimeInfoStoreThrowingIllegalArgumentException() throws Throwable {
		FutureTask<ActionEvent> futureTask = new FutureTask<>(new Callable<ActionEvent>() {
			@Override
			public ActionEvent call() throws Exception {
				long readOffset = 0l;
				KafkaReaderHandler kafkaReaderHandler = new KafkaReaderHandler();
				KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
				List<KafkaMessage> kafkaMessages = new ArrayList<>();
				KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				kafkaMessage = Mockito.mock(KafkaMessage.class);
				kafkaMessages.add(kafkaMessage);
				Mockito.when(kafkaSimpleConsumer.pollData(readOffset)).thenReturn(kafkaMessages);
				ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
				KafkaInputDescriptor descriptor = new KafkaInputDescriptor("unit-topic", 1);
				ReflectionTestUtils.setField(kafkaReaderHandler, "inputDescriptor", descriptor);
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				ReflectionTestUtils.setField(kafkaReaderHandler, "runtimeInfoStore", runtimeInfoStore);

				Mockito.when(runtimeInfoStore.get(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
						.thenThrow(new IllegalArgumentException(""));

				Status status = kafkaReaderHandler.process();
				Mockito.verify(kafkaSimpleConsumer, Mockito.times(1)).pollData(readOffset);
				Assert.assertSame(status, Status.CALLBACK);
				Assert.assertEquals(kafkaReaderHandler.getTotalInvocations(), 1);
				Assert.assertEquals(HandlerContext.get().getEventList().size(), 1);
				return null;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}
}
