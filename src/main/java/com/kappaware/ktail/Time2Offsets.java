package com.kappaware.ktail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Time2Offsets {
	static Logger log = LoggerFactory.getLogger(Time2Offsets.class);

	
	static void setOffsetToTs(KafkaConsumer<?, ?> consumer, List<TopicPartition> topicPartitions, Long ts) {
		try {
			setOffsetToTs10_1(consumer, topicPartitions, ts);
		} catch(UnsupportedVersionException e) {
			log.info("offsetsForTimes() not supported. Will fallback on dichotomous search.");
			setOffsetToTsPre10_1(consumer, topicPartitions, ts);
		}
	}

	
	
	/* Version for Kafka 0.10.1 and later (Does not works on 0.10.0 and before 
	 */ 
	static void setOffsetToTs10_1(KafkaConsumer<?, ?> consumer, List<TopicPartition> topicPartitions, Long ts) {
		Map<TopicPartition, Long> timestampByPartition = new HashMap<TopicPartition, Long>();
		for (TopicPartition tp : topicPartitions) {
			timestampByPartition.put(tp, ts);
		}
		Map<TopicPartition, OffsetAndTimestamp> offsetByPartition = consumer.offsetsForTimes(timestampByPartition);
		for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetByPartition.entrySet()) {
			if (entry.getValue() == null) {
				log.debug(String.format("No entry found. We are after the last message. Will seek to end"));
				Collection<TopicPartition> partitionAsList = Arrays.asList(new TopicPartition[] { entry.getKey() });
				consumer.seekToEnd(partitionAsList);
			} else {
				consumer.seek(entry.getKey(), entry.getValue().offset());
			}
		}
	}



	// We need some polling time, as even if there is some messages, polling with a short value may return an empty set.
	static private ConsumerRecord<?, ?> fetch(KafkaConsumer<?, ?> consumer) {
		ConsumerRecords<?, ?> records = consumer.poll(2000);
		if (records.count() == 0) {
			return null;
		} else {
			return records.iterator().next();
		}
	}

	/* Version for Kafka 0.10.0 and and previous. (Works on all version, but will be more slower) 
	 */ 

	static void setOffsetToTsPre10_1(KafkaConsumer<?, ?> consumer,List<TopicPartition> topicPartitions, Long ts) {
		consumer.pause(topicPartitions);
		for (TopicPartition part : topicPartitions) {
			List<TopicPartition> partAsList = Arrays.asList(new TopicPartition[] { part });
			consumer.resume(partAsList);
			// Loopkup first message
			consumer.seekToBeginning(partAsList);
			long firstOffset = consumer.position(part); // Never fail, as 0 if empty
			ConsumerRecord<?, ?> firstRecord = fetch(consumer);
			if (firstRecord == null) {
				log.debug(String.format("Partition#%d of topic %s is empty. Pointer set to begining", part.partition(), part.topic()));
				consumer.seekToBeginning(partAsList);
			} else {
				log.debug(String.format("Partition#%d of topic %s: First record at offset:%d  timestamp:%s", part.partition(), part.topic(), firstOffset, Utils.printIsoDateTime(firstRecord.timestamp())));
				consumer.seekToEnd(partAsList);
				long lastOffset = consumer.position(part) - 1;
				consumer.seek(part, lastOffset);
				ConsumerRecord<?, ?> lastRecord = fetch(consumer);
				log.debug(String.format("Partition#%d of topic %s: Last record at offset:%d  timestamp:%s", part.partition(), part.topic(), lastOffset, Utils.printIsoDateTime(lastRecord.timestamp())));
				long target = ts;
				if(firstRecord.timestamp() >= target) {
					log.debug(String.format("Partition#%d of topic %s: Timestamp %s is before first event (First event timestamp:%s). Pointer set to beginning", part.partition(), part.topic(), Utils.printIsoDateTime(target), Utils.printIsoDateTime(firstRecord.timestamp())));
					consumer.seekToBeginning(partAsList);
				} else if (lastRecord.timestamp() < target) {
					log.debug(String.format("Partition#%d of topic %s: Timestamp %s is not yet reached (Last event timestamp:%s). Pointer set to end", part.partition(), part.topic(), Utils.printIsoDateTime(target), Utils.printIsoDateTime(lastRecord.timestamp())));
					// This is same as seekToEnd, except if topic is populated in the same time
					consumer.seekToEnd(partAsList);
				} else {
					// So, at this point, we got
					// firstOffset and firstRecord
					// lastOffset and lastRecord
					// And target inside this interval
					// Will lookup appropriate offset, by dichotomous search
					int count = 0;
					while(lastOffset - firstOffset > 1) {
						if(count++ > 66) {
							// As offset range can't exceed 2^64, more than 64 loop is a bug symptom
							throw new RuntimeException(String.format("Too many loop in dichotomous offset lookup. Exiting!"));
						}
						long pivotOffset = (lastOffset + firstOffset) / 2;
						consumer.seek(part, pivotOffset);
						ConsumerRecord<?, ?> pivotRecord = fetch(consumer);
						if( pivotRecord.timestamp() >= target) {
							lastOffset = pivotOffset;
							lastRecord = pivotRecord;
						} else {
							firstOffset = pivotOffset;
							firstRecord = pivotRecord;
						}
					}
					log.debug(String.format("Dichotomous search converged in %d loops", count));
					log.debug(String.format("Partition#%d of topic %s: Pointer set at offset %d - timestamp: %s (previous: %s))", part.partition(), part.topic(), lastOffset, Utils.printIsoDateTime(lastRecord.timestamp()), Utils.printIsoDateTime(firstRecord.timestamp())));
					consumer.seek(part, lastOffset);
				}
			}
			consumer.pause(partAsList);
		}
		consumer.resume(topicPartitions);
	}
	
	
	
}
