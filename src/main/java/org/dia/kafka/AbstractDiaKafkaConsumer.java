/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

import static org.dia.kafka.Constants.*;

/**
 * AbstractDiaKafkaConsumer
 */
public abstract class AbstractDiaKafkaConsumer extends Thread {

    /**
     * Logger
     */
    public static final Logger LOG = LoggerFactory
            .getLogger(AbstractDiaKafkaConsumer.class);

    /**
     * Kafka consumer clientId
     */
    final static String clientId = "diaConsumerForSolr";
    /**
     * Kafka consumer
     */
    protected ConsumerConnector consumerConnector;

    private String kafkaTopic;
    private String zooUrl;

    /**
     * Initialize
     *
     * @param solrUrl
     * @param zooUrl
     * @param topic
     */
    public void initialize(String solrUrl, String solrCollection, String zooUrl, String topic) {
        this.kafkaTopic = topic;
        this.zooUrl = zooUrl;
        Properties properties = new Properties();
        properties.put("zookeeper.connect", Constants.ZOO_URL);
        properties.put("group.id", Constants.GROUP_ID);
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void initialize(String solrUrl, String solrCollection) {
        this.initialize(solrUrl, solrCollection, Constants.ZOO_URL, Constants.TOPIC);
    }

    /**
     * Initialize
     */
    public void initialize() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", ZOO_URL);
        properties.put("group.id", GROUP_ID + "_" + this.getClass().getSimpleName());
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void run() {
        LOG.info(String.format("[%s] started.", this.getClass().getSimpleName()));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int msgCnt = 0;
        while (it.hasNext()) {
            this.processMessage(new String(it.next().message()));
            LOG.debug("Msg#" + msgCnt);
        }
        LOG.info("[%s] ended.", this.getClass().getSimpleName());
    }

    /**
     * Method to be implemented by extending classes
     *
     * @param msg
     */
    public abstract void processMessage(String msg);

}
