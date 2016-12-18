/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.ktail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.ktail.TopicDesc.PartitionDesc;
import com.kappaware.ktail.config.Configuration;

public class Engine {
	static Logger log = LoggerFactory.getLogger(Engine.class);

	private Configuration configuration;
	private KafkaConsumer<String, String> consumer;

	Engine(Configuration config) {
		this.configuration = config;
		consumer = new KafkaConsumer<String, String>(this.configuration.getConsumerProperties(), new StringDeserializer(), new StringDeserializer());
	}

	void close() {
		this.consumer.close();
	}

	List<TopicDesc> listTopic() {
		Map<String, List<PartitionInfo>> partitionsByTopic = consumer.listTopics();
		List<TopicDesc> topics = new Vector<TopicDesc>();
		for (String topicName : partitionsByTopic.keySet()) {
			if (!topicName.startsWith("__")) {		// To skip __consumer_offsets
				TopicDesc topicDesc = new TopicDesc(topicName);
				topics.add(topicDesc);
				List<PartitionInfo> pi = partitionsByTopic.get(topicName);
				for (int p = 0; p < pi.size(); p++) {
					PartitionDesc partitionDesc = new PartitionDesc();
					topicDesc.addPartitionDesc(partitionDesc);
					TopicPartition topicPartition = new TopicPartition(topicName, p);
					List<TopicPartition> partAsList = Arrays.asList(new TopicPartition[] { topicPartition });
					consumer.assign(partAsList);
					// Loopkup first message
					consumer.seekToBeginning(partAsList);
					partitionDesc.setFirstOffset(consumer.position(topicPartition)); // Never fail, as 0 if empty
					ConsumerRecord<?, ?> firstRecord = fetchOne();
					if (firstRecord == null) {
						partitionDesc.setLastOffset(partitionDesc.getFirstOffset()); // Mark as empty
					} else {
						consumer.seekToEnd(partAsList);
						long lastOffset = consumer.position(topicPartition) - 1;
						partitionDesc.setLastOffset(lastOffset);
						consumer.seek(topicPartition, lastOffset);
						ConsumerRecord<?, ?> lastRecord = fetchOne();
						partitionDesc.setFirstTimestamp(firstRecord.timestamp());
						partitionDesc.setLastTimestamp(lastRecord.timestamp());
					}
				}
			}
		}
		return topics;
	}

	void tail() {
		List<PartitionInfo> partitions = consumer.partitionsFor(this.configuration.getTopic());
		List<TopicPartition> topicPartitions = new Vector<TopicPartition>();
		for (PartitionInfo p : partitions) {
			topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
		}
		this.consumer.assign(topicPartitions);
		if (configuration.getFromTimestamp() != null) {
			this.consumer.pause(topicPartitions);
			for (TopicPartition tp : topicPartitions) {
				rewindPartition(tp);
			}
			this.consumer.resume(topicPartitions);
		} else {
			consumer.seekToEnd(topicPartitions);
		}
		boolean running = true;
		long recordCount = 0;
		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(patternize(this.configuration.getPattern(), record));
				recordCount++;
				if (recordCount >= this.configuration.getMaxCount() || (this.configuration.getToTimestamp() != null && record.timestamp() > this.configuration.getToTimestamp())) {
					running = false;
					break;
				}
			}
		}
	}

	private String patternize(String pattern, ConsumerRecord<String, String> record) {
		StringBuffer sb = new StringBuffer();
		boolean inToken = false;
		for (int i = 0; i < pattern.length(); i++) {
			char c = pattern.charAt(i);
			if (inToken) {
				switch (c) {
					case '%':
						sb.append('%');
					break;
					case 'p':
						sb.append(Integer.toString(record.partition()));
					break;
					case 'o':
						sb.append(Long.toString(record.offset()));
					break;
					case 't':
						sb.append(Utils.printIsoDateTime(record.timestamp()));
					break;
					case 'k':
						sb.append(record.key() == null ? "-" : record.key());
					break;
					case 'v':
						sb.append(record.value() == null ? "-" : record.value());
					break;
					default:
						sb.append(c);
					break;
				}
				inToken = false;
			} else {
				if (c == '%') {
					inToken = true;
				} else {
					sb.append(c);
				}
			}

		}

		return sb.toString();
	}

	private void rewindPartition(TopicPartition part) {
		List<TopicPartition> partAsList = Arrays.asList(new TopicPartition[] { part });
		this.consumer.resume(partAsList);
		// Loopkup first message
		this.consumer.seekToBeginning(partAsList);
		long firstOffset = this.consumer.position(part); // Never fail, as 0 if empty
		ConsumerRecord<?, ?> firstRecord = this.fetchOne();
		if (firstRecord == null) {
			log.debug(String.format("Partition#%d of topic %s is empty. Pointer set to begining", part.partition(), part.topic()));
			this.setOffset(part, 0L);
		} else {
			log.debug(String.format("Partition#%d of topic %s: First record at offset:%d  timestamp:%s", part.partition(), part.topic(), firstOffset, Utils.printIsoDateTime(firstRecord.timestamp())));
			this.consumer.seekToEnd(partAsList);
			long lastOffset = this.consumer.position(part) - 1;
			this.consumer.seek(part, lastOffset);
			ConsumerRecord<?, ?> lastRecord = this.fetchOne();
			log.debug(String.format("Partition#%d of topic %s: Last record at offset:%d  timestamp:%s", part.partition(), part.topic(), lastOffset, Utils.printIsoDateTime(lastRecord.timestamp())));
			long target = this.configuration.getFromTimestamp();
			if (firstRecord.timestamp() >= target) {
				log.debug(String.format("Partition#%d of topic %s: Timestamp %s is before first event (First event timestamp:%s). Pointer set to beginning", part.partition(), part.topic(), Utils.printIsoDateTime(target), Utils.printIsoDateTime(firstRecord.timestamp())));
				this.setOffset(part, firstOffset);
			} else if (lastRecord.timestamp() < target) {
				log.debug(String.format("Partition#%d of topic %s: Timestamp %s is not yet reached (Last event timestamp:%s). Pointer set to end", part.partition(), part.topic(), Utils.printIsoDateTime(target), Utils.printIsoDateTime(lastRecord.timestamp())));
				// This is same as seekToEnd, except if topic is populated in the same time
				this.setOffset(part, lastOffset + 1);
			} else {
				// So, at this point, we got
				// firstOffset and firstRecord
				// lastOffset and lastRecord
				// And target inside this interval
				// Will lookup appropriate offset, by dichotonimus search
				int count = 0;
				while (lastOffset - firstOffset > 1) {
					if (count++ > 66) {
						// As offset range can't exceed 2^64, more than 64 loop is a bug symptom
						throw new RuntimeException(String.format("Too many loop in dichotomous offset lookup. Exiting!"));
					}
					long pivotOffset = (lastOffset + firstOffset) / 2;
					this.consumer.seek(part, pivotOffset);
					ConsumerRecord<?, ?> pivotRecord = this.fetchOne();
					if (pivotRecord.timestamp() >= target) {
						lastOffset = pivotOffset;
						lastRecord = pivotRecord;
					} else {
						firstOffset = pivotOffset;
						firstRecord = pivotRecord;
					}
				}
				log.info(String.format("Dichotomous search converged in %d loops", count));
				log.debug(String.format("Partition#%d of topic %s: Pointer set at offset %d - timestamp: %s (previous: %s))", part.partition(), part.topic(), lastOffset, Utils.printIsoDateTime(lastRecord.timestamp()), Utils.printIsoDateTime(firstRecord.timestamp())));
				this.setOffset(part, lastOffset);
			}
		}
		this.consumer.pause(partAsList);
	}

	private void setOffset(TopicPartition part, long l) {
		this.consumer.seek(part, l);
	}

	// We need some polling time, as even if there is some messages, polling with a short value may return an empty set (May be the time to seek() ?)
	// See KAFKA-3044. Note: Resolved means doc is adjusted to reflect the fact we have to wait.
	private ConsumerRecord<?, ?> fetchOne() {
		ConsumerRecords<?, ?> records = this.consumer.poll(2000);
		if (records.count() == 0) {
			return null;
		} else {
			return records.iterator().next();
		}
	}

}
