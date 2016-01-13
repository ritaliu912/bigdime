package io.bigdime.adaptor.metadata.impl;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger;
import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.repositories.DataTypeRepository;
import io.bigdime.adaptor.metadata.repositories.EntitiesRepository;
import io.bigdime.adaptor.metadata.repositories.MetadataRepository;

/**
 * 
 * MetadatastoreImpl interacts with this MetadataRepositoryService class to
 * perform CRUD operations with the configured data store. It utilizes all the
 * repositories available under io.bigdime.adaptor.metadata.repositories to
 * perform any kind of data base operations.
 * 
 * 
 * 
 * @author Neeraj Jain, psabinikari
 * 
 * @version 1.0
 * 
 */

@Component
public class MetadataRepositoryService {

	private static Logger logger = LoggerFactory
			.getLogger(MetadataRepositoryService.class);

	private static final double VERSIONINCREMENT = 0.1;

	private double repoVersion = 0.0;

	@Autowired
	private MetadataRepository repository;

	@Autowired
	private EntitiesRepository entityRepository;

	@Autowired
	private DataTypeRepository dataTypeRepository;

	private static final String SOURCENAME = "METADATA-API";

	@Value("${metastore.dynamicDataTypesConfiguration}")
	private boolean dynamicDataTypesConfiguration;

	/**
	 * Creates metasegment, entities and attributes entries in the repository.
	 * 
	 * @param metasegment
	 */
	public void createOrUpdateMetasegment(MetasegmentDTO metasegment,
			boolean inreaseVersion) {

		Assert.notNull(metasegment, "MetasegmentDTO object should not be null");

		Assert.hasText(metasegment.getAdaptorName(),
				"Application Name must not be null or empty");
		Assert.hasText(metasegment.getSchemaType(),
				"Schema Type must not be null or empty");

		MetasegmentDTO metaDBDetails = repository
				.findByAdaptorNameAndSchemaType(metasegment.getAdaptorName(),
						metasegment.getSchemaType());
		logger.debug(SOURCENAME,
				"Checking for metasegment existence before creation",
				"Metasegment Details from repository are {}", metaDBDetails);

		if (metaDBDetails != null) {
			if (metasegment.getEntitees() != null) {
				// Set<EntiteeDTO> entitees = metaDBDetails.getEntitees();
				logger.debug("SOURCENAME", "Metasegment size", metasegment
						.getEntitees().size() + "");
				for (EntiteeDTO entity : metasegment.getEntitees()) {

					logger.debug(SOURCENAME, "in entities",
							"Checking the entities");

					if (inreaseVersion) {

						entity.setVersion(repoVersion + VERSIONINCREMENT);

					}
					MetasegmentDTO latestMetaDBDetails = repository
							.findByAdaptorNameAndSchemaType(
									metasegment.getAdaptorName(),
									metasegment.getSchemaType());

					if (dynamicDataTypesConfiguration)
						configureDyanamicDataType(entity);
					// boolean removedEntityFlag = false;
					if (latestMetaDBDetails.getEntitees().size() > 0)
						for (EntiteeDTO repoEntity : latestMetaDBDetails
								.getEntitees())
							if (entity.getEntityName().equalsIgnoreCase(
									repoEntity.getEntityName())) {
								entity.setId(repoEntity.getId());
								latestMetaDBDetails.getEntitees().remove(
										repoEntity);
								// removedEntityFlag = true;
								break;
							}

					latestMetaDBDetails.getEntitees().add(entity);
					latestMetaDBDetails.setUpdatedAt(new Date());
					repository.save(latestMetaDBDetails);
				}
			}
			
		} else {
			if (metasegment.getEntitees() != null)
				for (EntiteeDTO entity : metasegment.getEntitees()) {
					if (dynamicDataTypesConfiguration)
						configureDyanamicDataType(entity);
				}
			repository.save(metasegment);
		}

		logger.debug(SOURCENAME, "Creation status",
				"successfully inserted into repository");
	}

	/**
	 * Inserts the data type details into Data_Type table dynamically.
	 * 
	 * @param entity
	 */
	public void configureDyanamicDataType(EntiteeDTO entity) {

		if (entity.getAttributes().isEmpty()) {
			logger.warn(
					SOURCENAME,
					"Configure Dynamic Data Type",
					"No Attributes exists to configure data type for Entity : {}",
					entity.getEntityName());
		} else {
			for (AttributeDTO attribute : entity.getAttributes()) {

				if (dataTypeRepository.findByDataType(attribute
						.getAttributeType()) == null) {

					DataTypeDTO dataType = new DataTypeDTO();
					dataType.setDataType(attribute.getAttributeType());
					dataType.setDescription(attribute.getAttributeName()
							+ " data type");
					dataTypeRepository.save(dataType);
				}
				attribute.setDataType(dataTypeRepository
						.findByDataType(attribute.getAttributeType()));

			}

		}

	}

	/**
	 * removes an entry from repository. If boolean is true, then it deletes at
	 * metasegment level along with its entities and attributes. If boolean is
	 * false, then it deletes an entity entry associated to metasegment.
	 * 
	 * @param metasegment
	 * @param deleteSegment
	 */
	public void remove(MetasegmentDTO metasegment, boolean deleteSegment) {

		Assert.notNull(metasegment);
		MetasegmentDTO metasegmentDetails = repository
				.findByAdaptorNameAndSchemaType(metasegment.getAdaptorName(),
						metasegment.getSchemaType());
		if (metasegmentDetails == null)
			logger.warn(
					SOURCENAME,
					"Remove Metasegment Details",
					"No details exists in the repository with adaptor name: {} and shema Type: {}",
					metasegment.getAdaptorName(), metasegment.getSchemaType());
		else if (deleteSegment) {

			repository.delete(metasegmentDetails);
			logger.debug(
					SOURCENAME,
					"Remove Metasegment Details",
					"Successfully deleted the metasegment details with application name: {} and schema type: {}",
					metasegment.getAdaptorName(), metasegment.getSchemaType());
		} else {
			if (metasegment.getEntitees() != null) {
				Set<EntiteeDTO> inputEntities = metasegment.getEntitees();
				Set<EntiteeDTO> repoEntities = metasegmentDetails.getEntitees();
				for (EntiteeDTO inputEntity : inputEntities) {
					for (EntiteeDTO deleteEntity : repoEntities) {
						if (inputEntity.getEntityName().equalsIgnoreCase(
								deleteEntity.getEntityName()))
							entityRepository.delete(deleteEntity.getId());

					}
				}
			}

		}
	}

	/**
	 * Checks Version eligibility before update
	 * 
	 * @param metasegment
	 * @return boolean(true/false)
	 */
	public boolean checkUpdateEligibility(MetasegmentDTO metasegment) {

		Assert.notNull(metasegment);
		int checkUpdateEligibilityCount = 0;
		MetasegmentDTO repoSegment = repository.findByAdaptorNameAndSchemaType(
				metasegment.getAdaptorName(), metasegment.getSchemaType());
		if (metasegment.getEntitees() != null) {
			for (EntiteeDTO entity : metasegment.getEntitees())
				for (EntiteeDTO repoEntity : repoSegment.getEntitees()) {
					if (entity.getVersion() <= repoEntity.getVersion())
						checkUpdateEligibilityCount++;
					repoVersion = repoEntity.getVersion();
				}

			if (checkUpdateEligibilityCount < metasegment.getEntitees().size())
				return false;
		}
		return true;
	}

	/**
	 * Checks the entity existence
	 * 
	 * @param metasegment
	 * @return boolean(true/false)
	 */
	public boolean schemaExists(MetasegmentDTO metasegment) {

		Assert.notNull(metasegment);
		Assert.hasText(metasegment.getAdaptorName(),
				"Application Name must not be null or empty");
		Assert.hasText(metasegment.getSchemaType(),
				"Schema Type must not be null or empty");
		boolean isSchemaExists = true;
		int repoSchemaExistCount = 0;
		MetasegmentDTO foundSegment = repository
				.findByAdaptorNameAndSchemaType(metasegment.getAdaptorName(),
						metasegment.getSchemaType());
		if (foundSegment != null
				&& foundSegment.getAdaptorName().equalsIgnoreCase(
						metasegment.getAdaptorName())
				&& foundSegment.getSchemaType().equalsIgnoreCase(
						metasegment.getSchemaType())) {
			logger.debug(
					SOURCENAME,
					"Inside SchemaExists()",
					"Metasegemnt for an adaptor : {} and schema type : {} exists in the repository",
					metasegment.getAdaptorName(), metasegment.getSchemaType());
			if (metasegment.getEntitees() != null) {
				for (EntiteeDTO entity : metasegment.getEntitees()) {
					for (EntiteeDTO repoEntity : foundSegment.getEntitees()) {

						if (entityRepository.findByIdAndEntityName(
								repoEntity.getId(), entity.getEntityName()) != null)
							repoSchemaExistCount++;

					}
				}

				if (repoSchemaExistCount < metasegment.getEntitees().size()) {
					isSchemaExists = false;
					logger.debug(SOURCENAME, "Schema Existence Check",
							"Schema doesn't exist in the repository");
				}
			}

		} else {
			logger.debug(
					SOURCENAME,
					"Schema Existence check",
					"Metasegemnt for an adaptor name: {} and schema type {} not exists in the repository",
					metasegment.getAdaptorName(), metasegment.getSchemaType());
			isSchemaExists = false;
		}
		return isSchemaExists;

	}

	public MetasegmentDTO checkAttributesAssociation(MetasegmentDTO metasegment) {
		Assert.notNull(metasegment);
		if (metasegment.getEntitees() != null) {
			Assert.notNull(metasegment.getEntitees());
			boolean columnExist = false;
			Set<EntiteeDTO> entityDTOSet = metasegment.getEntitees();

			MetasegmentDTO foundSegment = repository
					.findByAdaptorNameAndSchemaType(
							metasegment.getAdaptorName(),
							metasegment.getSchemaType());
			Set<EntiteeDTO> repoEntityDTOSet = foundSegment.getEntitees();
			for (EntiteeDTO entity : entityDTOSet) {
				for (EntiteeDTO repoEntity : repoEntityDTOSet) {
					if (entity.getEntityName().equalsIgnoreCase(
							repoEntity.getEntityName())) {
						Set<AttributeDTO> repoAttributeDTOSet = repoEntity
								.getAttributes();
						Set<AttributeDTO> attributeDTOSet = entity
								.getAttributes();
						for (AttributeDTO attribute : attributeDTOSet) {
							columnExist = false;
							for (AttributeDTO repoAttribute : repoAttributeDTOSet) {
								if (attribute.getAttributeName()
										.equalsIgnoreCase(
												repoAttribute
														.getAttributeName()))
									columnExist = true;

							}
							if (!columnExist)
								repoAttributeDTOSet.add(attribute);
						}

						attributeDTOSet.clear();
						attributeDTOSet.addAll(repoAttributeDTOSet);

					}
				}

			}

		}
		return metasegment;
	}

	/**
	 * Get the distinct data sources available in repository
	 * 
	 * @return distinct data sources list of strings available in repository if
	 *         exists else null object
	 */
	public Set<String> getDistinctDataSources() {

		logger.debug(SOURCENAME, "Distinct data Sources ",
				"Trying to fetch distinct data sources from repository");
		Set<String> distinctDataSources = repository.findAllDataSources();
		if (distinctDataSources.isEmpty())
			logger.debug(SOURCENAME, "Distinct data Sources",
					"No data sources exists in the repository");
		return distinctDataSources;
	}

	/**
	 * Get the Metasegment schemas available in repository
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @param entityName
	 * @return Metasegment or null
	 */
	public MetasegmentDTO getSchema(String adaptorName, String schemaType,
			String entityName) {
		Assert.hasText(adaptorName,
				"Application Name must not be null or empty");
		Assert.hasText(schemaType, "Schema Type must not be null or empty");
		Assert.hasText(entityName, "Entity Name must not be null or empty");
		logger.debug(SOURCENAME, "Get schema",
				"Getting the schema details available in repository");
		EntiteeDTO repositoryEntity = null;
		MetasegmentDTO foundSegment = null;
		Set<EntiteeDTO> entitiesSet = new HashSet<EntiteeDTO>();
		MetasegmentDTO repoSegment = repository.findByAdaptorNameAndSchemaType(
				adaptorName, schemaType);
		if (repoSegment == null) {

			logger.debug(
					SOURCENAME,
					"Get Schema",
					"Metasement with Application Name:{} and schemaType: {} is not available",
					adaptorName, schemaType);
		} else {
			logger.debug(SOURCENAME, "Get Schema",
					"metasegment details found and trying to find entity details");
			for (EntiteeDTO entity : repoSegment.getEntitees())
				repositoryEntity = entityRepository.findByIdAndEntityName(
						entity.getId(), entityName);
			if (repositoryEntity == null) {

				logger.debug(SOURCENAME, "Get schema ",
						"entity: {} is not associated with adaptor name: {} ",
						entityName, adaptorName);

			} else {
				entitiesSet.add(repositoryEntity);

			}

			foundSegment = new MetasegmentDTO();
			foundSegment.setId(repoSegment.getId());
			foundSegment.setAdaptorName(repoSegment.getAdaptorName());
			foundSegment.setDatabaseName(repoSegment.getDatabaseName());
			foundSegment.setDatabaseLocation(repoSegment.getDatabaseLocation());
			foundSegment.setRepositoryType(repoSegment.getRepositoryType());
			foundSegment.setIsDataSource(repoSegment.getIsDataSource());
			foundSegment.setDescription(repoSegment.getDescription());
			foundSegment.setEntitees(entitiesSet);
			foundSegment.setCreatedAt(repoSegment.getCreatedAt());
			foundSegment.setCreatedBy(repoSegment.getCreatedBy());
			foundSegment.setUpdatedBy(repoSegment.getUpdatedBy());
			foundSegment.setUpdatedAt(repoSegment.getUpdatedAt());

		}

		return foundSegment;

	}

	/**
	 * Queries Metasegment objects available in repository. if none available
	 * returns null object.
	 * 
	 * @return list of Metasegment objects if exists else null
	 */
	public List<MetasegmentDTO> getAllSegments() {
		logger.debug(SOURCENAME, "Get all segments",
				"Trying to fetch all the segments available in repository");
		List<MetasegmentDTO> metasegments = repository.findAll();
		logger.debug(SOURCENAME, "Get all segments",
				"metasegments that fetched from repository are {} ",
				metasegments);
		if (metasegments.isEmpty())
			logger.warn(SOURCENAME, "Get all segments",
					"No metasegments exists in the repository, hence returning null object");
		return metasegments;
	}

	/**
	 * Queries Metasegment objects available in repository for a given adaptor
	 * Name. if none available returns null object.
	 * 
	 * @return list of Metasegment objects if exists else null
	 */
	public List<MetasegmentDTO> getAllSegments(String adaptorName) {
		logger.debug(
				SOURCENAME,
				"Get all segments for a given adaptor Name",
				"Trying to fetch all the segments available in repository for adaptor Name: {}",
				adaptorName);
		List<MetasegmentDTO> metasegments = repository
				.findByAdaptorName(adaptorName);
		logger.debug(SOURCENAME, "Get all segments for a given adaptor Name",
				"metasegments that fetched from repository are {} ",
				metasegments);
		if (metasegments.isEmpty())
			logger.warn(
					SOURCENAME,
					"Get all segments for a given adaptor Name",
					"No metasegments exists in the repository for a given adaptor Name, hence returning null object");
		return metasegments;

	}

	/**
	 * 
	 * Get all the Entities(Tables) for a given adaptor andschemaType
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @return unique Entitee objects if exists else null
	 */
	public Set<EntiteeDTO> getAllEntites(String adaptorName, String schemaType) {

		MetasegmentDTO repoSegment = repository.findByAdaptorNameAndSchemaType(
				adaptorName, schemaType);
		if (repoSegment == null) {
			logger.debug(
					SOURCENAME,
					"Get all entities",
					"No Metasegment available in repository with adaptorName: {} and schemType: {}",
					adaptorName, schemaType);
			return null;
		} else if (repoSegment.getEntitees().isEmpty()) {
			logger.debug(
					SOURCENAME,
					"Get all entities",
					"No entities are associated with adaptorName: {} and schemaType: {}",
					adaptorName, schemaType);
			return null;
		} else
			return repoSegment.getEntitees();

	}

}