/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;

public class PartitionParserHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		PartitionParserHandler partitionNamesParserHandler = getPartitionNamesParserHandler();

		Assert.assertEquals(partitionNamesParserHandler.getRegex(), "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		Assert.assertEquals(partitionNamesParserHandler.getHeaderName(), ActionEventHeaderConstants.SOURCE_FILE_NAME);
		// Assert.assertEquals(partitionNamesParserHandler.getPartitionNames(),
		// "account, timestamp");
//		Assert.assertEquals(partitionNamesParserHandler.getTruncateCharacters(), "-:");
	}

	@Test
	public void testBuildWithoutHeaderNames() throws AdaptorConfigurationException {

		PartitionParserHandler partitionNamesParserHandler = new PartitionParserHandler();

		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(PartitionNamesParserHandlerConstants.REGEX, "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES, "account, timestamp");
		partitionNamesParserHandler.setPropertyMap(propertyMap);
		partitionNamesParserHandler.build();

		Assert.assertEquals(partitionNamesParserHandler.getRegex(), "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		Assert.assertEquals(partitionNamesParserHandler.getHeaderName(), ActionEventHeaderConstants.SOURCE_FILE_NAME);
		// Assert.assertEquals(partitionNamesParserHandler.getPartitionNames(),
		// "account, timestamp");
		Assert.assertEquals(partitionNamesParserHandler.getTruncateCharacters(), null);
	}

	private PartitionParserHandler getPartitionNamesParserHandler() throws AdaptorConfigurationException {
		PartitionParserHandler partitionNamesParserHandler = new PartitionParserHandler();

		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(PartitionNamesParserHandlerConstants.HEADER_NAME, ActionEventHeaderConstants.SOURCE_FILE_NAME);
		propertyMap.put(PartitionNamesParserHandlerConstants.REGEX, "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES, "account, timestamp");
//		propertyMap.put(PartitionNamesParserHandlerConstants.TRUNCATE_CHARACTERS, "-:");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME, "timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT, "MM-dd-yyyy");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT, "yyyyMMdd");
		partitionNamesParserHandler.setPropertyMap(propertyMap);
		partitionNamesParserHandler.build();
		return partitionNamesParserHandler;
	}

	// public static void main(String[] args) {
	// String regex = "(^.+_?\\d{4}_?\\d{7}_?)(\\d{2}-?\\d{2}-?\\d{4}).log$";
	// regex = "(.*)_((\\d{2})(?:-?)(\\d{2})-?(\\d{4})).log$";
	// regex = "(.*)_((\\d{2})(?:-?)(\\d{2})(?:-?)(\\d{4})).log$";
	// regex = "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$";
	// String rawString = "NetworkActivity_7937_3791772_11-03-2015.log";
	// // rawString ="NetworkImpression_7937_11-03-2015.log";
	// // rawString ="NetworkClick_7937_11-03-2015.log";
	// Pattern p = Pattern.compile(regex);
	// Matcher m = p.matcher(rawString);
	//
	// while (m.find()) {
	// System.out.println("group_count="+m.groupCount());
	// for (int i = 0; i <= m.groupCount(); i++) {
	// System.out.println(m.group(i));
	// }
	// // System.out.println(m.group(1));
	//
	// }
	//
	// }

	@Test
	public void testProcess() throws Throwable {
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				PartitionParserHandler partitionNamesParserHandler = getPartitionNamesParserHandler();
				HandlerContext context = HandlerContext.get();
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME,
						"NetworkActivity_7937_3791772_11-03-2015.log");
				context.createSingleItemEventList(actionEvent);
				Status status = partitionNamesParserHandler.process();
				return status;
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
