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
package org.dia.kafka.labkey.consumer;

import org.dia.kafka.AbstractDiaKafkaConsumer;
import org.dia.kafka.Constants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Map;

import static org.dia.kafka.Constants.*;

/**
 * Class for consuming from Kafka and pushing them into LabKey.
 */
public class LabkeyKafkaConsumer extends AbstractDiaKafkaConsumer {

    /**
     * Constructor
     *
     * @param solrUrl
     * @param zooUrl
     * @param topic
     */
    public LabkeyKafkaConsumer(String solrUrl, String solrCollection, String zooUrl, String topic) {
        initialize(solrUrl, solrCollection, zooUrl, topic);
    }

    public static void main(String[] args) {
        String solrCollection = DEFAULT_SOLR_COL;
        String solrUrl = SOLR_URL;
        String zooUrl = ZOO_URL;
        String kafkaTopic = KAFKA_TOPIC;

        // TODO Implement commons-cli
        String usage = "java -jar ./target/labkey-consumer.jar [--solr-url <url>] [--sorl-collection <collection>] [--zoo-url <url>] " +
                "[--project <Project Name>] [--kafka-topic <topic_name>]\n";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--solr-url")) {
                solrUrl = args[++i];
            } else if (args[i].equals("--sorl-collection")) {
                solrCollection = args[++i];
            } else if (args[i].equals("--zoo-url")) {
                zooUrl = args[++i];
            } else if (args[i].equals("--kafka-topic")) {
                kafkaTopic = args[++i];
            }
        }

        final LabkeyKafkaConsumer solrK = new LabkeyKafkaConsumer(solrUrl, solrCollection, zooUrl, kafkaTopic);

        // adding shutdown hook for shutdown gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                System.out.println();
                System.out.format("[%s] Exiting app.\n", solrK.getClass().getSimpleName());
                //solrK.closeProducer();
            }
        }));
        solrK.start();
    }

    @Override
    public void processMessage(String kafkaMsg) {
        // decide where the message is comming from
        JSONObject jsonObject = (JSONObject) JSONValue.parse(kafkaMsg);
        Constants.DataSource source = Constants.DataSource.valueOf(jsonObject.get(SOURCE_TAG).toString());
        JSONObject doc = null;
        switch (source) {
            // encode message into LabkeyFormat
            case OODT:
                doc = convertOodtMsgLabkey(jsonObject);
                break;
            case ISATOOLS:
                doc = convertIsaMsgLabkey(jsonObject);
                break;
            default:
                throw new IllegalArgumentException("Data source not supported yet, please try one "
                        + "of 'oodt', or 'isatools'. Additionally, please write to celgene-jpl@jpl.nasa.gov"
                        + "and let us know about new data sources you would like to see supported.");
        }
        // insert message into LabKey
        this.pushMsgToLabkey(doc);
    }

    /**
     * Maps ISATools fields into Labkey's fields
     *
     * @param jsonObject
     * @return
     */
    private JSONObject convertIsaMsgLabkey(JSONObject jsonObject) {
        JSONObject newJson = new JSONObject();
        for (Map.Entry entry : ISA_LABKEY.entrySet()) {
            newJson.put(entry.getValue(), jsonObject.get(entry.getKey()));
        }
        return newJson;
    }

    /**
     * * Maps OODT fields into Labkey's fields
     *
     * @param jsonObject
     * @return
     */
    private JSONObject convertOodtMsgLabkey(JSONObject jsonObject) {
        JSONObject newJson = new JSONObject();
        for (Map.Entry entry : OODT_LABKEY.entrySet()) {
            newJson.put(entry.getValue(), jsonObject.get(entry.getKey()));
        }
        return newJson;
    }

    /**
     * Pushes mapped object into Labkey
     *
     * @param doc
     */
    private void pushMsgToLabkey(JSONObject doc) {
        //TODO convertion of jsonObj into Labkey format
        LOG.error(String.format("[%] Msg <%s> not pushed into Labkey.", this.getClass().getSimpleName(), doc.toJSONString()));
    }
}
