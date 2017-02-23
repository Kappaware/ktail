# ktail

ktail is a small project intended to dump content of a Kafka topic from a given time.


***
## Usage

ktail is provided as rpm packages (Sorry, only this packaging is currently provided. Contribution welcome), on the [release pages](https://github.com/Kappaware/ktail/releases).

Once installed, basic usage is the following:

    # ktail --brokers "brk1.my.domain.com:9092,brk2.my.domain.com:9092,brk3.my.domain.com:9092" --list
  
or

    # ktail --brokers "brk1.my.domain.com:9092,brk2.my.domain.com:9092,brk3.my.domain.com:9092" --topic t1
    
Here all all the parameters

	Option (* = required)            Description
	---------------------            -----------
	* --brokers <br1:9092,br2:9092>  Comma separated values of Source Kafka brokers
	--forceProperties                Force unsafe properties
	--from <Starting point in time>  Start from (In Iso date format or in XX[d|h|m|s] notation)
	--help                           Display this usage message
	--list                           list available topics
	--max <Long: count>              Max record count (default: 9223372036854775807)
	--pattern <topic1>               Display pattern. ex: %p-%t-%o-%k-%v for <partition#>-<timestamp>-<offset>-<key>-<value> (default: %v)
	--property <prop=val>            Producer property (May be specified several times)
	--to <Ending point in time>      Up to (In Iso date format or in XX[d|h|m|s] notation)
	--topic <topic1>                 Source topic


***
## Some examples

	ktail --brokers "brk1.my.domain.com:9092" --topic t1 --from 2m 

List all messages from topic `t1` produced in the last 2 minutes and wait for new messages.

	ktail --brokers "brk1.my.domain.com:9092" --topic t1 --from 2m --to 0s

List all messages from topic `t1` produced in the last 2 minutes and exit

	ktail --brokers "brk1.my.domain.com:9092" --topic t1 --from 2017-01-01T12:00:00  --to 2017-01-01T13:00:00 --pattern "%k [%t]"

List all messages key and timestamp from topic `t1` produced between 12H and 13H on January 1rst 2017

***
## Kerberos support

If kerberos is activated, you will need to define a jaas configuration file as java option. 

This can easily be achieved by uncomment this following line in `/etc/ktail/setenv.sh`
    
    JOPTS="$JOPTS -Djava.security.auth.login.config=/etc/ktail/kafka_client_jaas.conf"
    
You can of course modify the `kafka_client_jaas.conf` file to adjust to your needs or target another existing one.

But, keep in mind, you must also perform a `kinit` command, with a principal granting read access to target topic

***
## Ansible integration

You will find an Ansible role [at this location](http://github.com/BROADSoftware/bsx-roles/tree/master/kappatools/ktail).

This role can be used as following;
	
	- hosts: kafka_brokers
	
	- hosts: cmd_node
	  vars:
        kdescribe_rpm_url: https://github.com/Kappaware/ktail/releases/download/v0.2.0/ktail-0.2.0-1.noarch.rpm
        kerberos: true
        kafka_port: 9092
	  roles:
	  - kappatools/ktail
	  
> Note: `- hosts: kafka_brokers` at the beginning, which force ansible to grab info about the hosts in the `[kafka_brokers]` group, to be able to fulfill this info into ktail configuration. Of course, such a group must be defined in the inventory. In this case, brokers information will no longer be required on the command line.


***
## Build

Just clone this repository and then:

    $ gradlew rpm

This should build everything. You should be able to find generated packages in build/distribution folder.


***
## License

    Copyright (C) 2017 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.




