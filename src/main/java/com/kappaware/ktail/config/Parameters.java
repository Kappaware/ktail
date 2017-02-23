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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import joptsimple.OptionSpec;

public class Parameters extends BaseParameters {
	static Logger log = LoggerFactory.getLogger(Parameters.class);

	private OptionSpec<String> BROKERS_OPT;
	private OptionSpec<String> TOPIC_OPT;
	private OptionSpec<?> LIST_TOPICS_OPT;
	private OptionSpec<String> PATTERN_OPT;
	private OptionSpec<String> FROM_OPT;
	private OptionSpec<String> TO_OPT;
	private OptionSpec<Long> MAX_COUNT_OPT;
	private OptionSpec<String> PROPERTY_OPT;
	private OptionSpec<?> FORCE_PROPERTIES_OPT;

	public Parameters(String[] argv) throws ConfigurationException, ParserHelpException {
		super();

		BROKERS_OPT = parser.accepts("brokers", "Comma separated values of Source Kafka brokers").withRequiredArg().describedAs("br1:9092,br2:9092").ofType(String.class).required();
		TOPIC_OPT = parser.accepts("topic", "Source topic").withRequiredArg().describedAs("topic1").ofType(String.class);
		LIST_TOPICS_OPT = parser.accepts("list", "list available topics");
		PATTERN_OPT = parser.accepts("pattern", "Display pattern. ex: %p-%t-%o-%k-%v for <partition#>-<timestamp>-<offset>-<key>-<value>").withRequiredArg().describedAs("topic1").ofType(String.class).defaultsTo("%v");
		PROPERTY_OPT = parser.accepts("property", "Producer property (May be specified several times)").withRequiredArg().describedAs("prop=val").ofType(String.class);
		FORCE_PROPERTIES_OPT = parser.accepts("forceProperties", "Force unsafe properties");

		
		FROM_OPT = parser.accepts("from", "Start from (In Iso date format or in XX[d|h|m|s] notation)").withRequiredArg().describedAs("Starting point in time").ofType(String.class);
		TO_OPT = parser.accepts("to", "Up to (In Iso date format or in XX[d|h|m|s] notation)").withRequiredArg().describedAs("Ending point in time").ofType(String.class);
		MAX_COUNT_OPT = parser.accepts("max", "Max record count").withRequiredArg().describedAs("count").ofType(Long.class).defaultsTo(Long.MAX_VALUE);
		
		
		this.parse(argv);
		
		if(!this.isListTopics() && !this.result.has(TOPIC_OPT)) {
			throw new ConfigurationException("Either --list or --topic <topic> must be defined!");
		}
	}


	public String getBrokers() {
		return result.valueOf(BROKERS_OPT);
	}

	public String getTopic() {
		return result.valueOf(TOPIC_OPT);
	}

	public String getFrom() {
		return result.valueOf(FROM_OPT);
	}
	
	public String getTo() {
		return result.valueOf(TO_OPT);
	}

	public boolean isListTopics() {
		return result.has(LIST_TOPICS_OPT);
	}
	
	public String getPattern() {
		return result.valueOf(PATTERN_OPT);
	}
	
	public Long getMaxCount() {
		return result.valueOf(MAX_COUNT_OPT);
	}
	
	public List<String> getProperties() {
		return result.valuesOf(PROPERTY_OPT);
	}

	public boolean isForceProperties() {
		return result.has(FORCE_PROPERTIES_OPT);
	}

	
}
