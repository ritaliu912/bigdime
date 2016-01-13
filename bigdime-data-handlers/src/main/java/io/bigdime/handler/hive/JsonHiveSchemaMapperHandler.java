/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hive;

import static io.bigdime.core.commons.DataConstants.COLUMN_SEPARATED_BY;
import static io.bigdime.core.commons.DataConstants.CTRL_A;
import static io.bigdime.core.commons.DataConstants.EOL;
import static io.bigdime.core.commons.DataConstants.ROW_SEPARATED_BY;
import static io.bigdime.core.commons.DataConstants.SCHEMA_FILE_NAME;
import static io.bigdime.core.commons.DataConstants.ENTITY_NAME;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.TimeManager;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.handler.kafka.KafkaInputDescriptor;

/**
 * 
 * @author mnamburi/jbrinnand/Neeraj Jain The handler class can be used when a Input
 *         Json to be flatten to stream of records based on the schema
 *         definition. The class depends on metadata of stream records, the
 *         stream record should be defined in the following format.
 */
@Component
@Scope("prototype")
public class JsonHiveSchemaMapperHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JsonHiveSchemaMapperHandler.class));

	private String columnSeparatedBy;
	private String rowSeparatedBy;
	private ObjectMapper objectMapper;

	private static final DateTimeZone timeZone = DateTimeZone.forID("UTC");
	private static final String DF = "yyyyMMdd";
	private static final String HOUR_FORMAT = "HH";
	private static final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern(HOUR_FORMAT).withZone(timeZone);
	private static final DateTimeFormatter formatter = DateTimeFormat.forPattern(DF).withZone(timeZone);
	private String schemaFileName;
	private String entityName;

	private final String ACCOUNT = "ACCOUNT";
	private final String TIMESTAMP = "DT";
	private final String HOUR = "HOUR";

	@Autowired
	private MetadataStore metadataStore;
	@Autowired
	private MetaDataJsonUtils metaDataJsonUtils;
	@Autowired
	private JsonHelper jsonHelper;
	private TimeManager timeManager = TimeManager.getInstance();
	private AtomicInteger atomicInteger = new AtomicInteger();
//	@Autowired
//	protected CounterService counterService;

	/**
	 * Using this for logging purpose only.
	 */
	private String handlerPhase;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building JsonHiveSchemaMapperHandler";
		logger.debug(handlerPhase, "building JsonHiveSchemaMapperHandler");
		Metasegment metaSegment = null;
		schemaFileName = PropertyHelper.getStringProperty(getPropertyMap(), SCHEMA_FILE_NAME);

		
		@SuppressWarnings("unchecked")
		Entry<Object, String> srcDescInputs = (Entry<Object, String>) getPropertyMap().get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		@SuppressWarnings("unchecked")
		Map<String,Object>  inputMetadata = (Map<String,Object>) srcDescInputs.getKey();
		
		KafkaInputDescriptor inputDescriptor = new KafkaInputDescriptor();
		try {
			inputDescriptor.parseDescriptor(inputMetadata);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					 "incorrect value specified in src-desc "+ ex.getMessage());
		}

		entityName = inputDescriptor.getEntityName();
		Preconditions.checkNotNull(entityName,"entityName should not be null");
		columnSeparatedBy = PropertyHelper.getStringProperty(getPropertyMap(), COLUMN_SEPARATED_BY, CTRL_A);
		rowSeparatedBy = PropertyHelper.getStringProperty(getPropertyMap(), ROW_SEPARATED_BY, EOL);
		objectMapper = new ObjectMapper();
		try {
			//synchronized (Metasegment.class) {
			metaSegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getName(), "HIVE",entityName);
			if (metaSegment == null) {
				metaSegment = buildMetaSegment();
				metadataStore.put(metaSegment);
			}
			//}
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing JsonHiveSchemaMapperHandler";
		logger.debug(handlerPhase, "processing JsonHiveSchemaMapperHandler");
//		if(counterService != null){
//			counterService.increment(JsonHiveSchemaMapperHandler.class.getName());
//		}
		if (getSimpleJournal().getEventList() != null && !getSimpleJournal().getEventList().isEmpty()) {
			List<ActionEvent> actionEvents = getSimpleJournal().getEventList();
			logger.debug(handlerPhase, "_message=\"journal not empty\" list_size={}", actionEvents.size());
			return processIt(actionEvents);
		} else {
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			logger.debug(handlerPhase, "_message=\"journal empty, will process from context\" actionEvents={}",
					actionEvents);
			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext must be not null");
			Preconditions.checkArgument(!actionEvents.isEmpty(),
					"eventList in HandlerContext must contain at least one ActionEvent");
			return processIt(actionEvents);
		}
	}

	/**
	 * 
	 */
	private Status processIt(List<ActionEvent> actionEvents) throws HandlerException {
		Status statusToReturn = Status.READY;
		Metasegment metaSegment = null;
		byte[] records = null;
		try {
			logger.debug(handlerPhase,"total Messages read ={} ",atomicInteger.incrementAndGet());
			metaSegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getName(), "HIVE",
					entityName);
			ActionEvent actionEvent = actionEvents.remove(0);

			JsonNode jn = objectMapper.readTree(actionEvent.getBody());
			String account = jsonHelper.getRequiredStringProperty(jn, "account");
			actionEvent.getHeaders().put(ACCOUNT, account);
			JsonNode context = jsonHelper.getRequiredNode(jn, "context");
			try {
				String serverTimestamp = context.get("serverTimestamp").getTextValue();
				DateTime dateTime = new DateTime(serverTimestamp, timeZone);
				String dt = formatter.print(dateTime);
				String hour = hourFormatter.print(dateTime);
				logger.debug(handlerPhase, "formatted date from message is: {} ", dt);
				actionEvent.getHeaders().put(TIMESTAMP, dt);
				actionEvent.getHeaders().put(HOUR, hour);
			} catch (Exception e) {
				DateTime dt = timeManager.getLocalDateTime();
				String dateFormat = timeManager.format(TimeManager.FORMAT_YYYYMMDD, dt);
				String hour = hourFormatter.print(dt);
				actionEvent.getHeaders().put(TIMESTAMP, dateFormat);
				actionEvent.getHeaders().put(HOUR, hour);
				
			}
			actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);

			//synchronized (Metasegment.class) {
			records = buildEventRecords(metaSegment, actionEvent.getBody());
			actionEvent.setBody(records);
			//}


			/*
			 * Check for outputChannel map. get the eventList of channels. check
			 * the criteria and put the message.
			 */
			logger.debug(handlerPhase, "checking channel submission, output_channel={}", getOutputChannel());
			if (getOutputChannel() != null) {
				logger.debug(handlerPhase, "submitting to channel");
				getOutputChannel().put(actionEvent);
			}

			getHandlerContext().createSingleItemEventList(actionEvent);
			if (!actionEvents.isEmpty()) {
				getSimpleJournal().setEventList(actionEvents);
				logger.debug(handlerPhase, "setting status to CALLBACK, actionEvents.size={}", actionEvents.size());
				statusToReturn = Status.CALLBACK;
			} else {
				getSimpleJournal().setEventList(null);
				logger.debug(handlerPhase, "setting status to READY, actionEvents.size={}", actionEvents.size());
				statusToReturn = Status.READY;
			}

		} catch (IOException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"json parser exception\" error={}", e.toString());
			throw new HandlerException("json parser exception", e);
		} catch (MetadataAccessException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"metadata access exception\" error={}", e.toString());
			throw new HandlerException("metadata access exception", e);
		}
		return statusToReturn;
	}

	private byte[] buildEventRecords(Metasegment metaSegment, byte[] eventMessage)
			throws IOException, MetadataAccessException {
		logger.debug(handlerPhase, "building event records");
		StringBuffer sb = new StringBuffer();

		Map<String, Object> eventRecordMap = new HashMap<String, Object>();
		JsonNode jn = objectMapper.readTree(eventMessage);
		JsonNode contextNode = jsonHelper.getRequiredNode(jn, "context");
		Map<String, Object> contestNodeMap = jsonHelper.getNodeTree(contextNode);
		Map<String, Object> propertyNodeMap = null;
		JsonNode eventsNode = jsonHelper.getRequiredArrayNode(jn, "events");
		eventRecordMap.putAll(contestNodeMap);
		if (eventsNode.isArray()) {
			for (JsonNode jsonNode : eventsNode) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				JsonNode propertyNode = jsonHelper.getRequiredNode(jsonNode, "properties");
				propertyNodeMap = jsonHelper.getNodeTree(propertyNode);
				eventRecordMap.putAll(propertyNodeMap);
				logger.debug(handlerPhase, "will build message buffer");
				buildMessageBuffer(metaSegment, eventRecordMap, baos);
				String tempOut = baos.toString(StandardCharsets.UTF_8.toString());
				int last = tempOut.lastIndexOf(columnSeparatedBy);
				String out = tempOut.substring(0, last) + rowSeparatedBy;
				sb.append(out);
				eventRecordMap.clear();
				baos.flush();
				baos.close();
				eventRecordMap.clear();
			}
		}
		byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
		return bytes;
	}

	private Metasegment buildMetaSegment() throws IOException {
		logger.debug(handlerPhase, "reading the schme from config location, schemaFileName={}",schemaFileName);
		JsonNode jsonNode = null;
		Metasegment metaSegment  = null;
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(schemaFileName)) {
			if (is == null) {
				throw new FileNotFoundException(schemaFileName);
			}
			jsonNode = objectMapper.readTree(is);
			metaSegment = metaDataJsonUtils.convertJsonToMetaData(AdaptorConfig.getInstance().getName(),entityName,
					jsonNode);
		}catch (IOException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INPUT_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"schema parsing exception \" error={}", e.toString());
			throw e;
		}
		return metaSegment;
	}

	/**
	 * @formatter:off
	 * Build Message buffer class supports to read an existing schema
	 * and flatten the structure, build the buffer and send it to caller.
	 *
	 * This method can be used when users wants to manipulate the data with schema.
	 * 
	 *schema =   { "name": "click-stream",
                    "version": "1.1.0",
                    "type": "map",
                    "clickStreamEventMessage": {
                        "hiveDatabase": {"name": "databaseName","location": "/data/clickstream"},
                        "hiveTable": { "name": "tableName", "type": "external", "location": "dataLocation},
                        "hivePartitions": {
                            "feed": {"name": "feed","type": "string","comments": ""},
                            "dt": {"name": "dt","type": "string", "comments": ""}
                        },
                        "properties": [{
                                "type": "eventProperties",
                                "account": { "name": "account","type": "string", "comments": ""},
                                 "userAgent": { "name": "useragent","type": "string", "comments": "TBD"}
                        }]
                    }
                 }
	 * msgEntry =  {"account": "desktop,
                        "events": [{
                            "name": "bannerDetails",
                            "properties": {
                            "customer_prop": "prop_value",
                        },
                        "timestamp": "2013-09-23T15:15:30Z"
                        }],
                        "context": {
                            "customer_context": "true"
                        }
                   }
	 * @param metaSegment
	 * @param msgEntry
	 * @throws IOException 
	 * @throws MetadataAccessException 
	 * @formatter:on
	 */
	public void buildMessageBuffer(Metasegment metaSegment, Map<String, Object> msgEntry, ByteArrayOutputStream baos)
			throws IOException, MetadataAccessException {
		// TODO : meta segment should return get entity for the table.
		logger.debug(handlerPhase, "building message buffer, msgEntry={}", msgEntry);
		Set<Attribute> attributes = null;
		Set<Attribute> newAttributes = null;
		int previousRecordsCount = 0;
		Entitee entitee = metaSegment.getEntity(entityName);
		if (entitee != null)
			attributes = entitee.getAttributes();

		Map<String, Object> tempMsgEntry = new LinkedHashMap<>();
		Set<Entry<String, Object>> entrySet = msgEntry.entrySet();
		for (Entry<String, Object> entry : entrySet) {
			tempMsgEntry.put(StringUtils.replaceChars(entry.getKey(), ":", "_").toLowerCase(), entry.getValue());
		}
		if(attributes != null){
			previousRecordsCount = attributes.size();
			for (Attribute attribute : attributes) {
				Object value = null;
				String type = attribute.getAttributeType();
				logger.debug(handlerPhase, "attribute.getAttributeName().toLowerCase()={} value={}",
						attribute.getAttributeName().toLowerCase(),
						tempMsgEntry.get(attribute.getAttributeName().toLowerCase()));
				value = tempMsgEntry.remove(attribute.getAttributeName());
				if (value != null) {
					if (type.equals("string")) {
						value = (String) value;
					} else if (type.equals("int")) {
						value = Integer.valueOf(value.toString());
					}
					String eval = value + columnSeparatedBy;
					baos.write(eval.getBytes());
				} else {
					String eval;
					if (type.equals("string")) {
						eval = "null" + columnSeparatedBy;
						baos.write(eval.getBytes());
					} else {
						baos.write(Integer.valueOf(0).intValue());
					}
				}
			}

		}
		/*
		 * If there are additional fields in the message sent by kafka message,
		 * they wont be present in schemaEntry. We need to dump them as it is
		 * and keep those property names in cache(newSchemaEntries).
		 */
		Attribute attribute = null;
		boolean isSchemaChanged = Boolean.FALSE;
		if (!tempMsgEntry.isEmpty()) {
			newAttributes = new LinkedHashSet<Attribute>();

			Set<Entry<String, Object>> otherProperties = tempMsgEntry.entrySet();
			Iterator<Entry<String, Object>> entryIterator = otherProperties.iterator();
			while (entryIterator.hasNext()) {
				Entry<String, Object> prop = entryIterator.next();
				logger.debug(handlerPhase, "key={} value={}", prop.getKey(), prop.getValue());
				attribute = new Attribute();
				attribute.setAttributeName(StringUtils.replaceChars(prop.getKey(), ":", "_").toLowerCase());
				attribute.setAttributeType("string");
				attribute.setComment("added by HiveMapper");
				newAttributes.add(attribute);
				logger.warn("_message=\"new property seen by hiveMapper\"",
						"property_name={} property_value={} thread_id={}", prop.getKey(), prop.getValue(),
						Thread.currentThread().getId());
				isSchemaChanged = Boolean.TRUE;
				logger.debug(handlerPhase,
						"_message=\"new attribute entry will be added\" key={} value={} attribute_name={}",
						prop.getKey(), prop.getValue(), attribute.getAttributeName());
				Object value = prop.getValue();
				entryIterator.remove();// why are we removing it?
				if (value == null)
					value = "null";
				value = value + columnSeparatedBy;
				baos.write(value.toString().getBytes());
			}
			if (isSchemaChanged) {
				// TODO : need to improve further, fixing as the job is failed.
				verifyAndRetryRecordsDiscrepency(baos,metaSegment,  msgEntry,previousRecordsCount,attributes.size());
				//synchronized (metadataStore) {
				Set<Attribute> tempAttributes  =  new LinkedHashSet<Attribute>();
				tempAttributes.addAll(attributes);
				tempAttributes.addAll(newAttributes);
				metaSegment.getEntity(entityName).setAttributes(tempAttributes);
				metadataStore.put(metaSegment);
				//}
			}

		}
		logger.debug(handlerPhase, "raw byte buffer is:{} ",
				StringEscapeUtils.escapeJava(baos.toString(StandardCharsets.UTF_8.toString())));
	}
	public void verifyAndRetryRecordsDiscrepency(ByteArrayOutputStream baos ,Metasegment metaSegment,
			Map<String, Object> msgEntry,int previousRecordsCount,int attributeRecordCount) throws IOException, MetadataAccessException{
		if(previousRecordsCount != attributeRecordCount){
			baos.reset();
			buildMessageBuffer(metaSegment,  msgEntry, baos);
			return;
		}
	}


	private SimpleJournal getSimpleJournal() throws HandlerException {
		SimpleJournal simpleJournal = getNonNullJournal(SimpleJournal.class);// getJournal(); = null;
		return simpleJournal;
	}
}
