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
package org.dia.kafka.isatools.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dia.kafka.AbstractDiaKafkaConsumer;
import org.dia.kafka.Constants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Map;

import static org.dia.kafka.Constants.*;

/**
 * Class for consuming from Kafka and pushing them into ISATools.
 */
public class ISAToolsKafkaConsumer extends AbstractDiaKafkaConsumer {

    /**
     * Constructor
     *
     * @param solrUrl Solr URL to use with this consumer
     * @param solrCollection Solr collection/core to use with this consumer
     * @param zooUrl ZooKeeper URL to use with this consumer
     * @param topic Kafka Topic to use with this consumer
     */
    public ISAToolsKafkaConsumer(String zooUrl, String topic) {
        initialize(zooUrl, topic);
    }

    public static void main(String[] args) {
        String zooUrl = ZOO_URL;
        String kafkaTopic = TOPIC;
        
        Options options = new Options();

        options.addOption("zo", "zoo-url", true, "The ZooKeeper URL to use with this consumer.");
        options.addOption("kt", "kafka-topic", true, "The Kafka Topic to use with this consumer.");
        
        CommandLineParser parser = new DefaultParser();;
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.getArgs().length != 5) {
                throw new ParseException("Did not see expected # of arguments, saw " + cmd.getArgs().length);
            }
        } catch (ParseException e) {
            System.err.println("Failed to parse command line " + e.getMessage());
            System.err.println();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(ISAToolsKafkaConsumer.class.getClass().getSimpleName() + " <zoo-url> <kafka-topic>", options);
            System.exit(-1);
        }
        ISAToolsKafkaConsumer solrK = new ISAToolsKafkaConsumer(
            (cmd.getOptionValue("zo") == null) ? zooUrl : cmd.getOptionValue("zo"), 
                (cmd.getOptionValue("kt") == null) ? kafkaTopic : cmd.getOptionValue("kt"));
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
                doc = convertOodtMsgIsa(jsonObject);
                break;
            case ISATOOLS:
                doc = convertLabkeyMsgIsa(jsonObject);
                break;
            default:
                throw new IllegalArgumentException("Data source not supported yet, please try one "
                        + "of 'oodt', or 'labkey'. Additionally, please write to celgene-jpl@jpl.nasa.gov"
                        + "and let us know about new data sources you would like to see supported.");
        }
        // insert message into LabKey
        this.pushMsgToIsaTool(doc);
    }

    /**
     * Maps ISATools fields into Labkey's fields
     *
     * @param jsonObject
     * @return
     */
    private JSONObject convertLabkeyMsgIsa(JSONObject jsonObject) {
        JSONObject newJson = new JSONObject();
        for (Map.Entry entry : LABKEY_ISA.entrySet()) {
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
    private JSONObject convertOodtMsgIsa(JSONObject jsonObject) {
        JSONObject newJson = new JSONObject();
        for (Map.Entry entry : OODT_ISA.entrySet()) {
            newJson.put(entry.getValue(), jsonObject.get(entry.getKey()));
        }
        return newJson;
    }

    /**
     * Pushes mapped object into Labkey
     *
     * @param doc
     */
    private void pushMsgToIsaTool(JSONObject doc) {
        //TODO convertion of jsonObj into ISATool format
        LOG.error(String.format("[%] Msg <%s> not pushed into ISATools.", this.getClass().getSimpleName(), doc.toJSONString()));
    }
}
