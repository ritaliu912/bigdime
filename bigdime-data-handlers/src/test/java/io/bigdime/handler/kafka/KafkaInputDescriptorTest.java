/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.kafka;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;

public class KafkaInputDescriptorTest {

	@Test
	public void testConstructor() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor("topic1", 1);
		Assert.assertEquals(kafkaInputDescriptor.getTopic(), "topic1");
		Assert.assertEquals(kafkaInputDescriptor.getPartition(), 1);
	}

	@Test
	public void testGettersAndSetters() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor();
		GetterSetterTestHelper.doTest(kafkaInputDescriptor, "topic", "topic-testGettersAndSetters");
		GetterSetterTestHelper.doTest(kafkaInputDescriptor, "partition", 1);
	}

	@Test
	public void testGetNext() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor();
		List<String> availableInputs = new ArrayList<>();
		availableInputs.add("topic1:0");
		String lastInput = "topic1:0";

		String nextDescriptor = kafkaInputDescriptor.getNext(availableInputs, lastInput);
		Assert.assertEquals(nextDescriptor, lastInput);
	}

	/**
	 * Assert that parsing "topic1:0" sets topic as topic1 and partition as 0.
	 */
	@Test
	public void testParseDescriptor() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor();
		String descriptor = "topic1:0";
		kafkaInputDescriptor.parseDescriptor(descriptor);
		Assert.assertEquals(kafkaInputDescriptor.getTopic(), "topic1");
		Assert.assertEquals(kafkaInputDescriptor.getPartition(), 0);
	}

	/**
	 * Assert that parsing null throws a NullPointerException.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "descriptor can't be null or empty")
	public void testParseDescriptorWithNullDescriptor() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor();
		kafkaInputDescriptor.parseDescriptor("");
	}

	/**
	 * Assert that parsing "topic1:0" sets topic as topic1 and partition as 0.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "descriptor must be in topic:partition format")
	public void testParseDescriptorWithInvalidDescriptor() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor();
		kafkaInputDescriptor.parseDescriptor("topic");
	}

	@Test
	public void testToString() {
		KafkaInputDescriptor kafkaInputDescriptor = new KafkaInputDescriptor();
		kafkaInputDescriptor.setTopic("unit-testToString");
		kafkaInputDescriptor.setPartition(1);
		String stringDescriptor = kafkaInputDescriptor.toString();
		Assert.assertEquals(stringDescriptor, "unit-testToString:1");
	}

}
