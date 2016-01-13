/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Component;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.JsonHelper;

/**
 * 
 * @author mnamburi this helper class supports to convert a Json schema to Meta
 *         Segment and vice versa.
 */
@Component
public class MetaDataJsonUtils {
	private static Logger logger = LoggerFactory.getLogger(MetaDataJsonUtils.class);
	private JsonHelper jsonHelper = new JsonHelper();
	private static String ENTITY_PROPERTIES = "entityProperties";
	private static String HIVE_DATABASE = "hiveDatabase";
	private static String HIVE_DATABASE_NAME = "name";
	private static String HIVE_DATABASE_LOCATION = "location";

	private static String HIVE_TABLE = "hiveTable";
	private static String HIVE_TABLE_NAME = "name";
	private static String HIVE_TABLE_LOCATION = "location";
	// private static String HIVE_PARTITIONS = "hivePartitions";
	private static String COLUMNS = "columns";
	private static String COLUMN_NAME = "name";
	private static String COLUMN_TYPE = "type";

	// private static String ENTITY_NAME = "entityName";

	private Map<String, Object> getProperties(JsonNode entityProperties) {
		JsonNode properties = jsonHelper.getOptionalNodeOrNull(entityProperties, "properties");
		if (properties == null)
			return new HashMap<String, Object>();
		else
			return jsonHelper.getNodeTree(properties);
	}

	// TODO : some of them are hard codded
	/**
	 * this method supports to convert the Json schema to MetaSegment.
	 * 
	 * @param applicationName
	 * @param schema
	 * @return
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	public Metasegment convertJsonToMetaData(String applicationName,String entityName, JsonNode schema) {
		Set<Entry<String, Object>> entries = null;
		Attribute attribute = null;
		String comment = null;
		Map<String, Object> trackingSchema = null;
		Set<Attribute> attributesSet = new LinkedHashSet<Attribute>();
		Entitee entities = null;
		Metasegment metasegment = null;
		Set<Entitee> entitySet = new HashSet<Entitee>();

		// TODO : need to clean this method further.
		JsonNode entityProperties = jsonHelper.getRequiredNode(schema, "entityProperties");
		JsonNode hiveDatabase = jsonHelper.getRequiredNode(entityProperties, "hiveDatabase");
		JsonNode hiveTable = jsonHelper.getRequiredNode(entityProperties, "hiveTable");
		JsonNode hivePartitions = jsonHelper.getRequiredNode(entityProperties, "hivePartitions");

		trackingSchema = getProperties(entityProperties);
		entries = trackingSchema.entrySet();
		for (Entry<String, Object> schemaEntry : entries) {
			String key = StringUtils.replaceChars(schemaEntry.getKey(), ":", "_").toLowerCase();
			String type = ((Map<String, Object>) schemaEntry.getValue()).get("type").toString();
			if (((Map<String, Object>) schemaEntry.getValue()).get("comment") != null) {
				comment = ((Map<String, Object>) schemaEntry.getValue()).get("comment").toString();
			}
			attribute = new Attribute(key, type, null, null, null, comment, "COLUMN", null, null);
			attributesSet.add(attribute);
		}

		entities = convertJsonToEntitee(applicationName,entityName, hiveTable);
		entities.setAttributes(attributesSet);
		entitySet.add(entities);

		metasegment = new Metasegment();
		metasegment.setAdaptorName(applicationName);
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName(jsonHelper.getRequiredStringProperty(hiveDatabase, "name"));
		metasegment.setDatabaseLocation(jsonHelper.getRequiredStringProperty(hiveDatabase, "location"));
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");
		metasegment.setCreatedAt(new Date());
		metasegment.setUpdatedAt(new Date());

		return metasegment;
	}

	public Entitee convertJsonToEntitee(String applicationName, String entityName,JsonNode hiveTable) {
		Entitee entitee = new Entitee();
		entitee.setEntityName(entityName);
		return entitee;

	}

	public Set<Attribute> mapJsonToAttributeSet(JsonNode entityProperty) {
		JsonNode columns = jsonHelper.getRequiredArrayNode(entityProperty, COLUMNS);
		Set<Attribute> attributesSet = new LinkedHashSet<Attribute>();
		for (JsonNode column : columns) {
			String columnName = jsonHelper.getRequiredStringProperty(column, COLUMN_NAME);
			String columnType = jsonHelper.getRequiredStringProperty(column, COLUMN_TYPE);
			Attribute attribute = new Attribute();
			attribute.setAttributeName(encodeAttributeName(columnName));
			attribute.setAttributeType(columnType);
			attribute.setFieldType("COLUMN");
			attributesSet.add(attribute);
		}
		return attributesSet;
	}

	public Entitee mapJsonToEntity(JsonNode entityProperty, String adaptorName, String schemaType) {
		Entitee entitee = new Entitee();
		JsonNode hiveTable = jsonHelper.getRequiredNode(entityProperty, HIVE_TABLE);
		entitee.setEntityName(jsonHelper.getRequiredStringProperty(hiveTable, HIVE_TABLE_NAME));
		entitee.setEntityLocation(jsonHelper.getRequiredStringProperty(hiveTable, HIVE_TABLE_LOCATION));
		entitee.setAttributes(mapJsonToAttributeSet(entityProperty));
		return entitee;
	}

	public Metasegment mapJsonToMetasegment(JsonNode entityProperty, String adaptorName, String schemaType) {
		Metasegment metasegment = new Metasegment();
		JsonNode hiveDatabase = jsonHelper.getRequiredNode(entityProperty, HIVE_DATABASE);
		String databaseName = jsonHelper.getRequiredStringProperty(hiveDatabase, HIVE_DATABASE_NAME);
		String databaseLocation = jsonHelper.getRequiredStringProperty(hiveDatabase, HIVE_DATABASE_LOCATION);

		metasegment.setAdaptorName(adaptorName);
		metasegment.setSchemaType(schemaType);
		metasegment.setDatabaseName(databaseName);
		metasegment.setDatabaseLocation(databaseLocation);
		metasegment.setEntitees(new HashSet<Entitee>());
		metasegment.getEntitees().add(mapJsonToEntity(entityProperty, adaptorName, schemaType));
		return metasegment;
	}

	public List<Metasegment> mapEntityPropertiesToMetasegmentList(String adaptorName, JsonNode entityProperties,
			String schemaType) {
		List<Metasegment> metasegmentList = new ArrayList<>();

		for (JsonNode entityProperty : entityProperties) {
			Metasegment metasegment = mapJsonToMetasegment(entityProperty, adaptorName, schemaType);
			metasegmentList.add(metasegment);
		}
		return metasegmentList;
	}

	/**
	 * Puts all the entities from the dbMetasegments into one Metasegment
	 * instance.
	 * 
	 * @param dbMetasegments
	 * @param adaptorName
	 * @return
	 */
	public Metasegment unifyMetasegment(List<Metasegment> dbMetasegments, String adaptorName) {
		Metasegment unifiedMetasegment = null;
		for (Metasegment metasegment : dbMetasegments) {
			logger.debug(adaptorName, "unifying metasegments", "adding a segment to unified object, size={}",
					metasegment.getEntitees().size());
			if (unifiedMetasegment == null) {
				unifiedMetasegment = metasegment;
			} else {
				unifiedMetasegment.getEntitees().addAll(metasegment.getEntitees());
			}
		}
		return unifiedMetasegment;
	}

	public List<Map<String, Metasegment>> readSchemaAndConvertToMetasegment(MetadataStore metadataStore,
			String adaptorName, String schemaType, String schemaFileName)
					throws JsonProcessingException, IOException, MetadataAccessException {
		List<Map<String, Metasegment>> metaSegments = new ArrayList<>();
		Map<String, Metasegment> entityNameToSchemaMetasegmentMap = new HashMap<>();
		Map<String, Metasegment> entityNameToDbMetasegmentMap = new HashMap<>();
		metaSegments.add(entityNameToSchemaMetasegmentMap);
		metaSegments.add(entityNameToDbMetasegmentMap);
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(schemaFileName)) {
			if (is == null) {
				throw new FileNotFoundException(schemaFileName);
			}
			// get all the metasegments for given adaptorName and schemaType
			List<Metasegment> dbMetasegments = metadataStore.getAdaptorMetasegments(adaptorName, schemaType);
			if (dbMetasegments == null)
				dbMetasegments = new ArrayList<>();

			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readTree(is);

			JsonNode entityProperties = jsonHelper.getRequiredArrayNode(jsonNode, ENTITY_PROPERTIES);
			/*
			 * Get the list of metasegments from entityProperties. Each
			 * entityProperty can have one Metasegment
			 */
			List<Metasegment> schemaMetasegments = mapEntityPropertiesToMetasegmentList(adaptorName, entityProperties,
					schemaType);

			Metasegment unifiedMetasegment = unifyMetasegment(dbMetasegments, adaptorName);
			logger.debug(adaptorName, "unifying metasegments",
					"adding a segment to unified object, unifiedMetasegment={}", unifiedMetasegment);
			boolean updated = false;
			List<String> newAttributesToLog = new ArrayList<>();
			for (Metasegment schemaMetasegment : schemaMetasegments) {
				Set<Entitee> schemaEntities = schemaMetasegment.getEntitees();
				String schemaEntityName = "";
				if (schemaEntities != null && !schemaEntities.isEmpty()) {
					Entitee schemaEntity = schemaEntities.iterator().next();
					schemaEntityName = schemaEntity.getEntityName();

					if (unifiedMetasegment == null) {
						logger.debug(adaptorName, "unifying metasegments",
								"unifiedMetasegment is null, will set first metasegment as unified");
						unifiedMetasegment = schemaMetasegment;
						unifiedMetasegment.setCreatedAt(new Date());
						unifiedMetasegment.setUpdatedAt(new Date());
						logger.debug(adaptorName, "updating metasegment", "first entity added, entity_name={}",
								schemaEntityName);
						updated = true;
						// continue;
					}
					entityNameToSchemaMetasegmentMap.put(schemaEntityName, schemaMetasegment);
					entityNameToDbMetasegmentMap.put(schemaEntityName, unifiedMetasegment);
					Entitee dbEntity = unifiedMetasegment.getEntity(schemaEntityName);
					// If the entity is not found in db, add it.
					if (dbEntity == null) {
						unifiedMetasegment.getEntitees().add(schemaEntity);
						logger.debug(adaptorName, "updating metasegment", "new entity added, entity_name={}",
								schemaEntityName);
						updated = true;
					} else {
						// if it was found in db, see if all the attributes from
						// schema are there in db. If they are not, add them.
						Set<Attribute> schemaAttributes = schemaEntity.getAttributes();
						// List<String> newAttributesToLog = new ArrayList<>();
						// boolean updated = false;
						for (Attribute schemaAttribute : schemaAttributes) {
							Set<Attribute> dbAttributes = dbEntity.getAttributes();
							boolean found = false;
							for (Attribute dbAttribute : dbAttributes) {
								logger.debug(adaptorName, "checking if the attribute exists in db",
										"schema_attribute_name()={} db_attribute_name={}",
										schemaAttribute.getAttributeName(), dbAttribute.getAttributeName());
								if (schemaAttribute.getAttributeName()
										.equalsIgnoreCase(dbAttribute.getAttributeName())) {
									logger.debug(adaptorName, "attibute found",
											"schema_attribute_name()={} db_attribute_name={}",
											schemaAttribute.getAttributeName(), dbAttribute.getAttributeName());
									found = true;
									break;
								}
							}
							if (!found) {
								newAttributesToLog.add(schemaAttribute.getAttributeName());
								logger.debug(adaptorName, "attibute not found", "schema_attribute_name()={}",
										schemaAttribute.getAttributeName());
								dbEntity.getAttributes().add(schemaAttribute);
								updated = true;
							}
						}
					}
				}
			}
			if (updated) {
				logger.debug(adaptorName, "updating metasegment", "new attributes added, new_attributes={}",
						newAttributesToLog);
				metadataStore.put(unifiedMetasegment);
			}

			/*
			 * @formatter:off
			 * Read schema file.
			 * get entityList from the schemaFile
			 * get all entities for adaptor, schemaType
			 * For each entity in schema file
			 * 	get metasegment from the list from db
			 * 	if the entity found in db
			 * 		add the attributes if needed
			 * 		set the flag as updated=true
			 * 	else
			 * 		add the entity to metasegment
			 * @formatter:on
			 * 
			 */

		}
		logger.debug(adaptorName, "returning from readSchemaAndConvertToMetasegment", "metaSegments.size={}",
				metaSegments.size());
		return metaSegments;
	}

	/**
	 * Create a map of attribute names to attribute value. The name comes from
	 * Metasegment.entitee.attributes and value comes from payload. It's
	 * understood that the order of attributes in Metasegment.entitee.attributes
	 * and in payload is same.
	 * 
	 * @param schemaMetasegment
	 * @param entityName
	 * @param fields
	 * @return
	 */
	public static Map<String, String> createAttributeNameValueMap(Metasegment schemaMetasegment, String entityName,
			String[] fields) {
		final Entitee schemaEntity = schemaMetasegment.getEntity(entityName);
		final Set<Attribute> schemaAttributes = schemaEntity.getAttributes();
		final Map<String, String> attributeNameValueMap = new HashMap<>();
		int index = 0;
		for (final Attribute attribute : schemaAttributes) {
			attributeNameValueMap.put(attribute.getAttributeName(), fields[index]);
			index++;
		}
		return attributeNameValueMap;
	}

	public String encodeAttributeName(String rawAttributeName) {
		return StringUtils.replaceChars(rawAttributeName, ":", "_").toLowerCase();
	}
}
