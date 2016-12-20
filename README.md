# ktail

ktail is a small project intended to dump content of a Kafka topic from a given time.

You can launch ktail by following command:

	java -jar ktail-0.1.0.jar --help

Which should give the following output:

	Option (* = required)            Description
	---------------------            -----------
	* --brokers <br1:9092,br2:9092>  Comma separated values of Source Kafka brokers
	--from <Starting point in time>  Start from (In Iso date format or in XX[d|h|m|s] notation)
	--help                           Display this usage message
	--list                           list available topics
	--max <Long: count>              Max record count (default: 9223372036854775807)
	--pattern <topic1>               Display pattern. ex: %t-%o-%k-%v for <timestamp>-<offset>-<key>-<value> (default: %v)
	--to <Ending point in time>      Up to (In Iso date format or in XX[d|h|m|s] notation)
	--topic <topic1>                 Source topic

***
## License

    Copyright (C) 2016 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.




