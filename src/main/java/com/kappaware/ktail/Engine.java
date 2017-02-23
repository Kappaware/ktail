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
//import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
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
			if (!topicName.startsWith("__")) { // To skip __consumer_offsets
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
			Time2Offsets.setOffsetToTs(this.consumer, topicPartitions, configuration.getFromTimestamp());
		} else {
			consumer.seekToEnd(topicPartitions);
		}
		boolean running = true;
		long recordCount = 0;
		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			if (records.count() > 0) {
				//log.debug("--------------------------------------------------------------------------------------------------------------------");
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(patternize(this.configuration.getPattern(), record));
					recordCount++;
					if (recordCount >= this.configuration.getMaxCount() || (this.configuration.getToTimestamp() != null && record.timestamp() > this.configuration.getToTimestamp())) {
						running = false;
						break;
					}
				}
			} else {
				// No record anymore. Should we stop waiting
				if (this.configuration.getToTimestamp() != null && System.currentTimeMillis() > this.configuration.getToTimestamp()) {
					running = false;
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

	// We need some polling time, as even if there is some messages, polling with a short value may return an empty set (May be the time to seek() ?)
	// See KAFKA-3044. Note: Resolved means doc is adjusted to reflect the fact we have to wait.
	private ConsumerRecord<?, ?> fetchOne() {
		ConsumerRecords<?, ?> records = this.consumer.poll(1000);
		if (records.count() == 0) {
			return null;
		} else {
			return records.iterator().next();
		}
	}

}
