/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.multiple.impl;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.alert.LoggerFactory;

public class MultipleLoggerFactoryTest {

	@Test
	public void testGetLogger() {
		Logger logger = LoggerFactory.getLogger("test");
		Assert.assertNotNull(logger);
		System.out.println(logger.getClass().toString());
		Assert.assertEquals("class io.bigdime.alert.multiple.impl.MultipleLoggerFactory$MultipleLogger",
				logger.getClass().toString());

		Logger loggerDup = LoggerFactory.getLogger("test");
		Assert.assertNotNull(loggerDup);

		Assert.assertSame(logger, loggerDup);
	}

	@Test
	public void testGetLogger2() {
		Logger logger = LoggerFactory.getLogger("test-unit");
		Assert.assertNotNull(logger);
		Assert.assertEquals("class io.bigdime.alert.multiple.impl.MultipleLoggerFactory$MultipleLogger",
				logger.getClass().toString());

		Logger loggerDup = LoggerFactory.getLogger("test-unit1");
		Assert.assertNotNull(loggerDup);

		Assert.assertNotEquals(logger, loggerDup);
	}

	@Test
	public void testAlert1() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.alert("unit-test-source", ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER, "message");

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAlert2() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");

			logger.alert("unit-test-source", ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER, "message", new Exception("unit-exception"));
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAlert3() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			AlertMessage alertMessage = new AlertMessage();
			alertMessage.setAdaptorName("unit-test-source");
			alertMessage.setCause(ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
			alertMessage.setMessage("unit-message");
			alertMessage.setMessageContext("context");
			alertMessage.setSeverity(ALERT_SEVERITY.BLOCKER);
			alertMessage.setType(ALERT_TYPE.ADAPTOR_FAILED_TO_START);
			Assert.assertEquals(alertMessage.getMessageContext(), "context");
			logger.alert(alertMessage);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAlert4() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");

			logger.alert("unit-test-source", ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER, "field_1={} field_2={}", "field1", "field2");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testDebug1() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.debug("unit-test-source", "unit-short-message", "unit-long-message");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testDebug2() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.debug("unit-test-source", null, "field_a={} field_b={}", "unit1", "unit2");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testInfo1() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.info("unit-test-source", "unit-short-message", "unit-long-message");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testInfo2() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.info("unit-test-source", "unit-short-message", "unit-long-message", "field_a={} field_b={}", "unit1",
					"unit2");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testWarn1() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.warn("unit-test-source", "unit-short-message", "unit-long-message");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testWarn2() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.warn("unit-test-source", "", "unit-long-message", "field_a={} field_b={}", "unit1", "unit2");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testWarn3() {
		try {
			Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
			logger.warn("unit-test-source", "unit-short-message", "unit-long-message", new Exception("unit-exception"));
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testInfoWithException() {
		try {
			BigdimeHBaseLogger logger = (BigdimeHBaseLogger) BigdimeHBaseLogger
					.getLogger("MultipleLoggerFactoryTest.class");
			HbaseManager mockHbaseManager = Mockito.mock(HbaseManager.class);
			ReflectionTestUtils.setField(logger, "hbaseManager", mockHbaseManager);
			Mockito.doThrow(HBaseClientException.class).when(mockHbaseManager)
					.insertData(Mockito.any(DataInsertionSpecification.class));
			logger.info("unit-test-source", "unit-short-message", "unit-long-message");
			Thread.sleep(100);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		} 
	}

	@Test
	public void testAlertWithException() {
		try {
			BigdimeHBaseLogger logger = (BigdimeHBaseLogger) BigdimeHBaseLogger
					.getLogger("MultipleLoggerFactoryTest.class");
			AlertMessage alertMessage = new AlertMessage();
			alertMessage.setAdaptorName("unit-test-source");
			alertMessage.setCause(ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
			alertMessage.setMessage("unit-message");
			alertMessage.setMessageContext("context");
			alertMessage.setSeverity(ALERT_SEVERITY.BLOCKER);
			alertMessage.setType(ALERT_TYPE.ADAPTOR_FAILED_TO_START);
			Assert.assertEquals(alertMessage.getMessageContext(), "context");
			HbaseManager mockHbaseManager = Mockito.mock(HbaseManager.class);
			ReflectionTestUtils.setField(logger, "hbaseManager", mockHbaseManager);
			Mockito.doThrow(HBaseClientException.class).when(mockHbaseManager)
					.insertData(Mockito.any(DataInsertionSpecification.class));
			logger.alert(alertMessage);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
