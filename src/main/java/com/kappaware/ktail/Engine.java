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

	List<String> listTopic() {
		Map<String, List<PartitionInfo>> t = consumer.listTopics();
		return new Vector<String>(t.keySet());
	}

	void tail() {
		List<PartitionInfo> partitions = consumer.partitionsFor(this.configuration.getTopic());
		List<TopicPartition> topicPartitions = new Vector<TopicPartition>();
		for (PartitionInfo p : partitions) {
			topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
		}
		if (configuration.getTimestamp() != null) {
			this.consumer.pause(topicPartitions);
			for (TopicPartition tp : topicPartitions) {
				rewindPartition(tp);
			}
			this.consumer.resume(topicPartitions);
		} else {
			consumer.seekToEnd(topicPartitions);
		}
		this.consumer.assign(topicPartitions);
		ConsumerRecords<String, String> records = consumer.poll(100);
		for (ConsumerRecord<String, String> record : records) {
			System.out.println(patternize(this.configuration.getPattern(), record));
		}
	}

	private String patternize(String pattern, ConsumerRecord<String, String> record) {
		StringBuffer sb = new StringBuffer();
		boolean inToken = false;
		for(int i = 0; i < pattern.length(); i++) {
			char c = pattern.charAt(i);
			if(inToken) {
				switch(c) {
					case '%':
						sb.append('%');
					break;
					case 'p':
						sb.append(Integer.toString(record.partition()));
					break;
					case 'o':
						sb.append(Long.toString(record.offset()));
					break;
					case 't' :
						sb.append(Utils.printIsoDateTime(record.timestamp()));
					break;
					case 'k' :
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
				if(c ==  '%') {
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
			log.info(String.format("Partition#%d of topic %s is empty. Pointer set to begining", part.partition(), part.topic()));
			this.setOffset(part, 0L);
		} else {
			log.info(String.format("Partition#%d of topic %s: First record at offset:%d  timestamp:%s", part.partition(), part.topic(), firstOffset, Utils.printIsoDateTime(firstRecord.timestamp())));
			this.consumer.seekToEnd(partAsList);
			long lastOffset = this.consumer.position(part) - 1;
			this.consumer.seek(part, lastOffset);
			ConsumerRecord<?, ?> lastRecord = this.fetchOne();
			log.info(String.format("Partition#%d of topic %s: Last record at offset:%d  timestamp:%s", part.partition(), part.topic(), lastOffset, Utils.printIsoDateTime(lastRecord.timestamp())));
			long target = this.configuration.getTimestamp();
			if (firstRecord.timestamp() >= target) {
				log.info(String.format("Partition#%d of topic %s: Timestamp %s is before first event (First event timestamp:%s). Pointer set to beginning", part.partition(), part.topic(), Utils.printIsoDateTime(target), Utils.printIsoDateTime(firstRecord.timestamp())));
				this.setOffset(part, firstOffset);
			} else if (lastRecord.timestamp() < target) {
				log.info(String.format("Partition#%d of topic %s: Timestamp %s is not yet reached (Last event timestamp:%s). Pointer set to end", part.partition(), part.topic(), Utils.printIsoDateTime(target), Utils.printIsoDateTime(lastRecord.timestamp())));
				// This is same as seekToEnd, except if topic is populated in the same time
				this.setOffset(part, lastOffset + 1);
			} else {
				// So, at this point, we got
				// firstOffset and firstRecord
				// lastOffset and lastRecord
				// And target inside this interval
				// Will lookup appropriate offset, by dichotomous search
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
				log.info(String.format("Partition#%d of topic %s: Pointer set at offset %d - timestamp: %s (previous: %s))", part.partition(), part.topic(), lastOffset, Utils.printIsoDateTime(lastRecord.timestamp()), Utils.printIsoDateTime(firstRecord.timestamp())));
				this.setOffset(part, lastOffset);
			}
		}
		this.consumer.pause(partAsList);
	}

	private void setOffset(TopicPartition part, long l) {
		this.consumer.seek(part, l);
	}

	// We need some polling time, as even if there is some messages, polling with a short value may return an empty set.
	private ConsumerRecord<?, ?> fetchOne() {
		ConsumerRecords<?, ?> records = this.consumer.poll(2000);
		if (records.count() == 0) {
			return null;
		} else {
			return records.iterator().next();
		}
	}

}
