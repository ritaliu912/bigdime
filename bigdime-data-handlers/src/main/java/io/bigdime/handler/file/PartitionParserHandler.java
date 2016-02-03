/**
 * Copyright (C) 2015 Stubhub.
 */

package io.bigdime.handler.file;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;

/**
 * 
 * 
 * This handler parses the partition names from the input file name.
 * 
 * @formatter:off
 * date-partition-name parameter, if specified, reads the value of field specified by this name.
 * date-partition-input-format and date-partition-output-format must be specified if date-partition-name is specified.
 * 
 * If the date-partition-name parameter is specified, the value of the field is read, parsed based on value specified by date-partition-input-format and then formats based on value specified by date-partition-output-format. 
 * @formatter:on
 * 
 * @author Neeraj Jain
 * 
 * 
 */
@Component
@Scope("prototype")
public class PartitionParserHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(PartitionParserHandler.class));
	private String regex;
	private String headerName;
	private String handlerPhase;
	private char[] truncateChars;
	private String truncateCharacters;
	private String partitionNames;

	private String datePartitionName;
	private DateTimeFormatter datePartitionInputFormatter;
	private DateTimeFormatter datePartitionOutputFormatter;
	private int datePartitionIndex = 0;

	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building PartitionParserHandler";
		logger.info(handlerPhase, "handler_name={} handler_id={} \"properties={}\"", getName(), getId(),
				getPropertyMap());
		super.build();
		regex = PropertyHelper.getStringProperty(getPropertyMap(), PartitionNamesParserHandlerConstants.REGEX);
		Preconditions.checkNotNull(regex,
				PartitionNamesParserHandlerConstants.REGEX + " must be configured in handler properties");
		partitionNames = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.PARTITION_NAMES);
		Preconditions.checkNotNull(partitionNames,
				PartitionNamesParserHandlerConstants.PARTITION_NAMES + "  must be configured in handler properties");

		headerName = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.HEADER_NAME);
		if (StringUtils.isBlank(headerName)) {
			headerName = ActionEventHeaderConstants.SOURCE_FILE_NAME;
		}
		truncateCharacters = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.TRUNCATE_CHARACTERS);
		if (truncateCharacters != null) {
			truncateChars = truncateCharacters.toCharArray();
		} else {
			truncateChars = new char[0];
		}

		datePartitionName = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME);
		String datePartitionInputFormat = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT);
		String datePartitionOutputFormat = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT);
		logger.debug(handlerPhase,
				"headerId={} datePartitionName={} datePartitionInputFormat={} datePartitionOutputFormat={}", getId(),
				datePartitionName, datePartitionInputFormat, datePartitionOutputFormat);

		if (datePartitionName != null) {
			Preconditions.checkNotNull(datePartitionInputFormat,
					PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT
							+ "  must be configured in handler properties if "
							+ PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME + " is specified");
			Preconditions.checkNotNull(datePartitionInputFormat,
					PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT
							+ "  must be configured in handler properties if "
							+ PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME + " is specified");

			datePartitionName = StringUtils.trim(datePartitionName);
			datePartitionInputFormatter = DateTimeFormat.forPattern(datePartitionInputFormat);

			datePartitionOutputFormatter = DateTimeFormat.forPattern(datePartitionOutputFormat);

			String[] partitionNameArray = partitionNames.split(",");
			for (int i = 0; i < partitionNameArray.length; i++) {
				if (StringUtils.trim(partitionNameArray[i]).equals(datePartitionName)) {
					datePartitionIndex = i;
					logger.debug(handlerPhase, "headerId={} datePartitionIndex={}", getId(), datePartitionIndex);
					break;
				}
			}
		}

		logger.debug(handlerPhase, "headerId={} headerName={} regex={} truncateCharacters={}", getId(), headerName,
				regex, truncateCharacters);
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing PartitionParserHandler";
		Pattern p = Pattern.compile(regex);

		List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		Preconditions.checkNotNull(actionEvents, "ActionEvents can't be null");
		Preconditions.checkArgument(!actionEvents.isEmpty(), "ActionEvents can't be empty");

		if (datePartitionName != null) {

		}
		for (ActionEvent actionEvent : actionEvents) {
			String rawString = actionEvent.getHeaders().get(headerName);
			logger.debug(handlerPhase, "handler_id={} rawString={}", getId(), rawString);

			Matcher m = p.matcher(rawString);
			String partitionValues = "";
			StringBuilder partitionValuesSb = null;
			while (m.find()) {
				logger.debug(handlerPhase, "found groups, count={}", m.groupCount());
				for (int i = 1; i <= m.groupCount(); i++) {
					String temp = m.group(i);
					for (char ch : truncateChars) {
						temp = temp.replace(String.valueOf(ch), "");
					}
					if (partitionValuesSb == null) {
						partitionValuesSb = new StringBuilder();
					} else {
						partitionValuesSb.append(",");
					}
					if (datePartitionName != null && (i - 1) == datePartitionIndex) {
						logger.debug(handlerPhase, "handler_id={} inputTimestamp={}", getId(), temp);

						DateTime dt = datePartitionInputFormatter.parseDateTime(temp);
						String temp1 = datePartitionOutputFormatter.print(dt).trim();
						logger.debug(handlerPhase, "handler_id={} inputTimestamp={} outputTimestamp={}", getId(), temp,
								temp1);
						temp = temp1;
					}
					partitionValuesSb.append(temp);
				}
			}
			partitionValues = partitionValuesSb.toString();
			logger.debug(handlerPhase, "handler_id={} partitionNames={} partitionValues={}", getId(), partitionNames,
					partitionValues);
			logger.debug(handlerPhase, "handler_id={} partitionValues={}", getId(), partitionValues);
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, partitionNames);
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, partitionValues);
			processChannelSubmission(actionEvent);
		}
		return Status.READY;
	}

	public String getRegex() {
		return regex;
	}

	public String getHeaderName() {
		return headerName;
	}

	public String getPartitionNames() {
		return partitionNames;
	}

	public String getTruncateCharacters() {
		return truncateCharacters;
	}
}
