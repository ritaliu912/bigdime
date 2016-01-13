/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.kafka;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.TimeManager;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.constants.KafkaReaderHandlerConstants;
import io.bigdime.libs.kafka.consumers.KafkaMessage;
import io.bigdime.libs.kafka.consumers.KafkaSimpleConsumer;
import io.bigdime.libs.kafka.exceptions.KafkaReaderException;
import io.bigdime.libs.kafka.utills.KafkaUtils;

/**
 * KafkaReaderHandler reads from one or more kafka topics.
 *
 * @author Neeraj Jain
 *
 */

@Component
@Scope("prototype")
public class KafkaReaderHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(KafkaReaderHandler.class));
	private long totalInvocations;
	private static final String KAFKA_MESSAGE_READER_OFFSET = "kafka_message_offset";
	//	@Autowired
	//	protected CounterService counterService;
	private TimeManager timeManager = TimeManager.getInstance();
	private static final String DF = "yyyyMMdd";
	private static final String HOUR_FORMAT = "HH";	
	private static final DateTimeZone timeZone = DateTimeZone.forID("UTC");
	
	private static final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern(HOUR_FORMAT).withZone(timeZone);
	private static final DateTimeFormatter formatter = DateTimeFormat.forPattern(DF).withZone(timeZone);
	private final String TIMESTAMP = "DT";
	private final String HOUR = "HOUR";

	/**
	 * KakfaConsumer component that's used to fetch data from Kafka.
	 */
	private KafkaSimpleConsumer kafkaSimpleConsumer;

	private KafkaInputDescriptor inputDescriptor;
	/**
	 * Messages fetched from Kafka. These need to be stored at the object level,
	 * since there will be multiple iterations needed to process all the
	 * messages in the list.
	 */
	private List<KafkaMessage> kafkaMessages;

	private String handlerPhase;

	private long currentOffset;
	private long  lastOffset;
	/**
	 * Each instance of handler reads from exactly one topic:partition.
	 */
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building Kafka Reader Handler";
		logger.info(handlerPhase, "handler_id={} handler_name={} properties={}", getId(), getName(), getPropertyMap());
		String brokers = PropertyHelper.getStringProperty(getPropertyMap(), KafkaReaderHandlerConstants.BROKERS);


		logger.debug(handlerPhase, "KafkaReader={}", this.toString());

		@SuppressWarnings("unchecked")
		Entry<Object, String> srcDescInputs = (Entry<Object, String>) getPropertyMap().get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		
		@SuppressWarnings("unchecked")
		Map<String,Object>  inputMetadata = (Map<String,Object>) srcDescInputs.getKey();
		
		inputDescriptor = new KafkaInputDescriptor();
		try {
			inputDescriptor.parseDescriptor(inputMetadata);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					 "incorrect value specified in src-desc "+ ex.getMessage());
		}

		try {
			currentOffset = getOffsetFromRuntimeInfo(runtimeInfoStore,inputDescriptor.getEntityName(), String.valueOf(inputDescriptor.getPartition()), KAFKA_MESSAGE_READER_OFFSET);
		} catch (RuntimeInfoStoreException e) {
			throw new AdaptorConfigurationException(e);
		}

		kafkaSimpleConsumer = KafkaSimpleConsumer.getInstance().brokers(KafkaUtils.parseBrokers(brokers, null))
				.clientId(AdaptorConfig.getInstance().getName()).topic(inputDescriptor.getTopic())
				.partitionId(String.valueOf(inputDescriptor.getPartition())).build();
		logger.info(handlerPhase, "topic:partition={} input_field_name={} currentOffset = {} ", srcDescInputs.getKey(),srcDescInputs.getValue(),currentOffset);
	}

	/**
	 * Get a list of messages. put send one message at a time in event. put info
	 * in the context
	 *
	 * Read context, see if it's new. If NEW set new = false If readSize ==
	 * totolSize set DONE = true;
	 */

	@Override
	public ActionEvent.Status process() throws HandlerException {
		try {
			handlerPhase = "processing KafkaReaderHandler";
			incrementTotalInvocations();
			Status status = preProcess();
			if (status == Status.BACKOFF) {
				return status;
			}
			return process0();
		} catch (RuntimeInfoStoreException e) {
			throw new HandlerException("Unable to process message from Kafka", e);
		}
	}

	private void incrementTotalInvocations() {
		totalInvocations++;
	}

	private Status preProcess() throws RuntimeInfoStoreException , HandlerException{

		if (shallProcessNew()) {
			logger.debug(handlerPhase, "will process new batch");
			try {
				if(currentOffset == -1){
					kafkaMessages = kafkaSimpleConsumer.pollData(kafkaSimpleConsumer.getEarliestOffSet());
				} else{
					kafkaMessages = kafkaSimpleConsumer.pollData(currentOffset);
				}
			} catch (KafkaReaderException e) {
				logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
						"\"kafka reader exception\" error={}", e.toString());
				throw new HandlerException("Unable to read the messages from Kafka", e);
			}			
			if (kafkaMessages == null || kafkaMessages.isEmpty()) {
				logger.info(handlerPhase, "no data in the kafka , will BACKOFF");
				return Status.BACKOFF;
			}

			currentOffset = kafkaMessages.get(kafkaMessages.size()-1).getOffset()+1;
			lastOffset = kafkaMessages.get(kafkaMessages.size()-1).getOffset();

			logger.debug(handlerPhase, "kafkaMessageOffsetFromRuntimeInfo={} kafkaMessages.get(0).getOffset()={}",
					currentOffset, kafkaMessages.get(0).getOffset());

			int nextIndexInKafkaMessagesList = 0;
			/*
			 * Kafka offsets are sequential. If we were processing a list of 100
			 * messages, and got errored out after, say processing 30 messages,
			 * we need to start from 31st message. So, we just subtract the
			 * offset of the first message from the offset we receive from
			 * runtime info store.
			 * 
			 * If, the offset of the first message in the list is greater than
			 * kafkaMessageOffsetFromRuntimeInfo, then we'd load the first
			 * message.
			 * 
			 * If the offset from runtime store is bigger than the offset of the
			 * last message from kafka, no need to process any message, so
			 * BACKOFF.
			 */

			if (lastOffset >= kafkaMessages.get(0).getOffset()) {
				if (lastOffset > kafkaMessages.get(kafkaMessages.size() - 1).getOffset()) {
					return Status.BACKOFF;
				} else {
					nextIndexInKafkaMessagesList = 0;
				}
			}

			long totalSize = kafkaMessages.size();
			getSimpleJournal().setTotalSize(totalSize);
			getSimpleJournal().setTotalRead(nextIndexInKafkaMessagesList);
			logger.debug(handlerPhase, "handlerContext={} total_size={} total_read={}", getHandlerContext(),
					getTotalSizeFromJournal(), getTotalReadFromJournal());
			if (shallProcessNew()) {
				return Status.BACKOFF;
			}
			return Status.READY;
		}
		return Status.READY;
	}

	private Status process0()  throws HandlerException {
		//		if(counterService != null){
		//			counterService.increment(KafkaReaderHandler.class.getName());
		//		}		
		ActionEvent actionEvent = new ActionEvent();
		long nextIndexToRead = getTotalReadFromJournal();// handlerContext.getTotalRead();
		logger.debug(handlerPhase, "nextIndexToRead={}", nextIndexToRead);
		KafkaMessage kafkaMessage = kafkaMessages.get((int) nextIndexToRead);
		long messageOffset = kafkaMessage.getOffset();

		byte[] body = kafkaMessage.getMessage();
		actionEvent.setBody(body);

		getHandlerContext().createSingleItemEventList(actionEvent);
		long totalRead = nextIndexToRead + 1;
		getSimpleJournal().setTotalRead(totalRead);

		actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, inputDescriptor.getTopic());
		actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR,
				String.valueOf(inputDescriptor.getPartition()));
		actionEvent.getHeaders().put(KAFKA_MESSAGE_READER_OFFSET, String.valueOf(messageOffset));

		DateTime dt = timeManager.getLocalDateTime();
		String dateFormat = timeManager.format(TimeManager.FORMAT_YYYYMMDD, dt);
		String hour = hourFormatter.print(dt);
		actionEvent.getHeaders().put(TIMESTAMP, dateFormat);
		actionEvent.getHeaders().put(HOUR, hour);
		
		processChannelSubmission(actionEvent);

		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			logger.info(handlerPhase, "totalRead from Journal ={}", getTotalSizeFromJournal());

			return Status.READY;
		} else {
			return Status.CALLBACK;
		}
	}
	/**
	 * get run time information from the metastore
	 * if the data is not available in the metastore, return the -1 says the is need to be processed from the earliest offset information.
	 * otherwise process the data where is is left off.
	 * @param entityName
	 * @param descriptor
	 * @param offsetPropertyName
	 * @return
	 */
	protected long getOffsetFromRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
			final String entityName, final String descriptor, final String offsetPropertyName)
					throws RuntimeInfoStoreException {
		try {
			RuntimeInfo runtimeInfo = runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName,
					descriptor);
			if (runtimeInfo != null && runtimeInfo.getProperties() != null) {
				long offsetFromRuntimeInfo = PropertyHelper
						.getLongProperty(runtimeInfo.getProperties().get(offsetPropertyName), -1);
				logger.debug("proessing handler", "MessageOffsetFromRuntimeInfo from runtime info={}",
						offsetFromRuntimeInfo);
				return offsetFromRuntimeInfo;
			} else {
				return -1;
			}
		} catch (IllegalArgumentException ex) {
			return -1;
		}
	}

	public long getTotalInvocations() {
		return totalInvocations;
	}

	public KafkaInputDescriptor getInputDescriptor() {
		return inputDescriptor;
	}

	private long getTotalReadFromJournal() throws HandlerException {
		HandlerJournal simpleJournal = getSimpleJournal();
		return simpleJournal.getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		HandlerJournal simpleJournal = getSimpleJournal();
		return simpleJournal.getTotalSize();
	}

	private boolean shallProcessNew() throws HandlerException {
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}

	private HandlerJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}

	protected String getHandlerPhase() {
		return handlerPhase;
	}
	/**
	 * if in case error out, reset the offset from run time information and process the messages.
	 */
	@Override
	public void handleException() {
		//currentOffset = getOffsetFromRuntimeInfo(runtimeInfoStore,inputDescriptor.getTopic(), String.valueOf(inputDescriptor.getPartition()), KAFKA_MESSAGE_READER_OFFSET);
	}
}
