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

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

import static org.dia.kafka.Constants.SERIALIZER;

/**
 * AbstractDIAKafkaProducer
 */
public class AbstractDiaKafkaProducer {
    /**
     * Kafka producer
     */
    protected kafka.javaapi.producer.Producer<String, String> producer;
    private volatile boolean running = true;
    private String kafkaTopic;

    /**
     * Kafka producer default initializer
     */
    protected void initializeKafkaProducer(String kafkaTopic, String kafkaUrl) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", kafkaUrl);
        properties.put("serializer.class", SERIALIZER);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.kafkaTopic = kafkaTopic;
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
    }

    /**
     * Kafka producer initializer
     */
    protected void initializeKafkaProducer(Properties properties) {
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.kafkaTopic = Constants.KAFKA_TOPIC;
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
    }

    /**
     * Sends messages into Kafka
     */
    protected void sendKafka(String message) {
        this.producer.send(new KeyedMessage<String, String>(this.kafkaTopic, message));
    }

    /**
     * Closes kafka producer
     */
    public void closeProducer() {
        this.producer.close();
    }

    /**
     * Wait for l seconds
     * @param l
     */
    protected void politeWait(long l) {
        try {
            Thread.sleep(l);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
