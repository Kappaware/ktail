package com.kappaware.ktail.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ConsumerPropertiesHelper {

	// @formatter:off
	static Set<String> validConsumerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 
		ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 
		ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
		ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 
		ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 
		ConsumerConfig.METADATA_MAX_AGE_CONFIG,
		ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 
		ConsumerConfig.SEND_BUFFER_CONFIG, 
		ConsumerConfig.RECEIVE_BUFFER_CONFIG, 
		ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 
		ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,
		ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 
		ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 
		ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
		ConsumerConfig.CHECK_CRCS_CONFIG, 
		ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
		ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
		ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
		ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
		ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
		ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
		ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
		ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
		ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG,
		ConsumerConfig.CLIENT_ID_CONFIG,
		CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
	}));

	static Set<String> protectedConsumerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ConsumerConfig.GROUP_ID_CONFIG, 
		ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
		ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
		ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
		ConsumerConfig.CLIENT_ID_CONFIG,
		ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
	}));
	
	public static Properties buildProperties(List<String> propertyStrings, String brokers, boolean forceProperties) throws ConfigurationException {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
		properties.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("ktail"));
		// Very specific to dichotomous search application.
		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

		for (String prop : propertyStrings) {
			String[] prp = prop.trim().split("=");
			if (prp.length != 2) {
				throw new ConfigurationException(String.format("Property must be as name=value. Found '%s'", prop));
			}
			String propName = prp[0].trim();
			if (forceProperties) {
				properties.put(propName, prp[1].trim());
			} else {
				if (validConsumerProperties.contains(propName)) {
					properties.put(propName, prp[1].trim());
				} else if (protectedConsumerProperties.contains(propName)) {
					throw new ConfigurationException(String.format("Usage of property '%s' is reserved by this application!", propName));
				} else {
					throw new ConfigurationException(String.format("Invalid property '%s'!", propName));
				}
			}
		}
		return properties;
	}

	
}
