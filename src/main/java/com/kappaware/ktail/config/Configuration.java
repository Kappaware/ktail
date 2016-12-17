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
package com.kappaware.ktail.config;

import java.util.Calendar;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
	static Logger log = LoggerFactory.getLogger(Configuration.class);

	
	private Parameters parameters;
	private Long timestamp;
	private Properties consumerProperties;

	public Configuration(Parameters parameters) throws ConfigurationException {
		this.parameters = parameters;
		if (this.parameters.getTimestamp() != null) {
			try {
				Calendar c = DatatypeConverter.parseDateTime(this.parameters.getTimestamp());
				this.timestamp = c.getTimeInMillis();
			} catch (Throwable t) {
				throw new ConfigurationException(String.format("'%s' is not a valid ISO 8601 datetime expression. It must be like 2015-12-31T13:00:00+01:00 or 2015-12-31T12:00:00Z)", this.parameters.getTimestamp()));
			}
		} else if (this.parameters.getBack() != null) {
			this.timestamp = System.currentTimeMillis();
			String s = this.parameters.getBack();
			try {
				char c = s.charAt(s.length() - 1);
				Long x = Long.parseLong(s.substring(0, s.length() - 1));
				switch(c) {
					case 's':
						this.timestamp -= x * 1000;
					break;
					case 'm':
						this.timestamp -= x * 60000;
					break;
					case'h':
						this.timestamp -= 3600000;
					break;
					case 'd':
						this.timestamp -= 3600000 * 24;
					break;
					default:
						throw new Exception();
				}
			} catch(Throwable t) {
				log.error(String.format("Unable to parse %s as a duration value. Must be a number followed by s,m,h or d. (e.i 30s or 2j)", s));
			}
		} else {
			this.timestamp = null;
		}
		this.consumerProperties = new Properties();
		this.consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.parameters.getBrokers());
		this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		this.consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("ktail"));
		// Very specific to rewind application
		this.consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
	}

	public Parameters getParameters() {
		return parameters;
	}
	
	// --------------------------------------

	public String getBrokers() {
		return parameters.getBrokers();
	}


	public String getTopic() {
		return parameters.getTopic();
	}
	
	public Long getTimestamp() {
		return timestamp;
	}

	public Properties getConsumerProperties() {
		return consumerProperties;
	}

	public boolean isListTopic() {
		return this.parameters.isListTopics();
	}
	
	public String getPattern() {
		return this.parameters.getPattern();
	}
}
