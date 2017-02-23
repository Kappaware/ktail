
# Set JAVA_HOME. Must be at least 1.7.
# If not set, will try to lookup a correct version.
# JAVA_HOME=/some/place/where/to/find/java

# Set the log configuration file
JOPTS="$JOPTS -Dlog4j.configuration=file:/etc/ktail/log4j.xml"



# Set brokers quorum
# OPTS="$OPTS --brokers br1:9092,br2:9092,br3:9092"

# If kerberos is activated, you must add line like this:
# JOPTS="$JOPTS -Djava.security.auth.login.config=/etc/ktail/kafka_client_jaas.conf"
# (You can of course modify the kafka_client_jaas.conf file to adjust to your needs or target another existing one).

# And also, still for kerberos, a line like that.
# OPTS="$OPTS --property security.protocol=SASL_PLAINTEXT"
# This may need to be adjusted, depending of your broker config
 

# But, keep in mind, you must also perform a kinit command, with a principal granting access to the topic 
#

