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


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.ktail.config.Configuration;
import com.kappaware.ktail.config.ConfigurationException;
import com.kappaware.ktail.config.Parameters;
import com.kappaware.ktail.config.ParserHelpException;



public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	
	static public void main(String[] argv) {
		log.info("ktail start");
		log.debug("ktail DEBUG enabled");
		try {
			Configuration config = new Configuration(new Parameters(argv));
			Engine engine = new Engine(config);
			if(config.isListTopic()) {
				List<String> topics = engine.listTopic();
				for(String topic: topics) {
					System.out.println(topic);
				}
			} else {
				engine.tail();
			}
			engine.close();
			System.exit(0);
		} catch (ConfigurationException e) {
			log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		} catch (ParserHelpException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		} 
	}
	
}
