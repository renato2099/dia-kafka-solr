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

import org.dia.kafka.AbstractDiaKafkaConsumer;
import org.dia.kafka.Constants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.dia.kafka.Constants.*;

/**
 * Class for consuming from Kafka and pushing them into ISATools.
 */
public class ISAToolsKafkaConsumer extends AbstractDiaKafkaConsumer {

  private static final String ISATOOLS_KAFKA_DEFAULT_PROPERTIES_FILE = "isatools-kafka.properties";

  private static final Logger LOG = LoggerFactory.getLogger(ISAToolsKafkaConsumer.class);

  /**
   * Constructor
   *
   * @param solrUrl Solr URL to use with this consumer
   * @param solrCollection Solr collection/core to use with this consumer
   * @param zooUrl ZooKeeper URL to use with this consumer
   * @param topic Kafka Topic to use with this consumer
   */
  public ISAToolsKafkaConsumer(String kafkaTopic, String zooUrl) {
    this.kafkaTopic = kafkaTopic;
    this.zooUrl = zooUrl;
  }

  public static void main(String[] args) {
    Properties props = new Properties();
    InputStream stream = ISAToolsKafkaConsumer.class.getClassLoader()
        .getResourceAsStream(ISATOOLS_KAFKA_DEFAULT_PROPERTIES_FILE);
    if(stream != null) {
      try {
        props.load(stream);
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          stream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } else {
      LOG.warn(ISATOOLS_KAFKA_DEFAULT_PROPERTIES_FILE + " not found, properties will be empty.");
    }
    ISAToolsKafkaConsumer solrK = new ISAToolsKafkaConsumer
        (props.getProperty("isatools.kafka.zookeeper", Constants.ZOO_URL), 
            props.getProperty("isatools.kafka.topic", Constants.KAFKA_TOPIC));
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
