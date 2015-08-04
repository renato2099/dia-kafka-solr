# dia-kafka-solr

# Introduction
A messaging system implementation which enables realtime [Apache Solr](http://lucene.apache.org/solr) 
indexing of stream data generated from incoming updates to a number of data sources inlcuding 
[Labkey](https://www.labkey.org/project/home/begin.view?), [Apache OODT](http://oodt.apache.org) and 
[ISATools](http://www.isa-tools.org/). 

Data integration is processed and managed using [Apache Kafka](http://kafka.apache.org). This 
enables realtime updates, data consistency and integration across the above applications and servers. 

# Compilation

Run mvn clean package from the top level directory.
Inside dia-kafka-solr/target you will the following executables:
* Producers
  - labkey-producer:
        java -jar ./target/labkey-producer.jar [--url <url>] [--user <user/email>] [--pass <pass>] [--project <Project Name>] [--wait <secs>] [--kafka-topic <topic_name>] [--kafka-url]
  - isatools-producer
        java -jar ./target/isatools-producer.jar [--tikaRESTURL <url>] [--isaToolsDir <dir>] [--wait <secs>] [--kafka-topic <topic_name>] [--kafka-url]
* Consumers
  - solr-consumer
        java -jar ./target/solr-consumer.jar [--solr-url <url>] [--sorl-collection <collection>] [--zoo-url <url>] [--project <Project Name>] [--kafka-topic <topic_name>]

# License

dia-kafka-solr is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)

# Contact
