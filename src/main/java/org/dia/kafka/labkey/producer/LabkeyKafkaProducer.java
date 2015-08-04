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

package org.dia.kafka.labkey.producer;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import com.google.common.collect.Lists;
import org.dia.kafka.AbstractDiaKafkaProducer;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.dia.kafka.Constants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.labkey.remoteapi.CommandException;
import org.labkey.remoteapi.Connection;
import org.labkey.remoteapi.query.ContainerFilter;
import org.labkey.remoteapi.query.SelectRowsCommand;
import org.labkey.remoteapi.query.SelectRowsResponse;

/**
 * Gets studies from labKey and puts them into kafka
 */
public class LabkeyKafkaProducer extends AbstractDiaKafkaProducer {

  /**
   * Tag for specifying things coming out of LABKEY
   */
  public final static String LABKEY_SOURCE_VAL = "LABKEY";
  /**
   * Previously seen UUIDs
   */
  private static Set<UUID> PREV_SEEN = new HashSet<UUID>();

  /**
   * Labkey db connection
   */
  private Connection connection = null;

  /**
   * Top level where studies will be obtained
   */
  public static final String TOP_PROJECT = "/";

  /**
   * Constructor
   */
  public LabkeyKafkaProducer(URL labkeyUrl, String username, String password, String kafkaTopic, String kafkaUrl) {
    this.connection = new Connection(labkeyUrl.toString(), username, password);
    this.initializeKafkaProducer(kafkaTopic, kafkaUrl);
  }

  /**
   * Constructor
   */
  public LabkeyKafkaProducer(URL labkeyUrl, String username, String password, Properties properties,  String kafkaTopic, String kafkaUrl) {
    this.connection = new Connection(labkeyUrl.toString(), username, password);
    this.initializeKafkaProducer(kafkaTopic, kafkaUrl);
  }

  /**
   * Executes LabkeyKafkaProducer as a single run
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String url = null;
    String user = null;
    String pass = null;
    String kafkaTopic = Constants.KAFKA_TOPIC;
    String kafkaUrl = Constants.KAFKA_URL;
    String projectName = TOP_PROJECT;
    long waitTime = Constants.DEFAULT_WAIT;

    // TODO Implement commons-cli
    String usage = "java -jar ./target/labkey-producer.jar [--url <url>] [--user <user/email>] [--pass <pass>] " +
        "[--project <Project Name>] [--wait <secs>] [--kafka-topic <topic_name>] [--kafka-url]\n";

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--url")) {
        url = args[++i];
      } else if (args[i].equals("--user")) {
        user = args[++i];
      } else if (args[i].equals("--pass")) {
        pass = args[++i];
      } else if (args[i].equals("--project")) {
        projectName = args[++i];
      } else if (args[i].equals("--wait")) {
        waitTime = Long.valueOf(args[++i]);
      } else if (args[i].equals("--kafka-topic")) {
        kafkaTopic = args[++i];
      } else if (args[i].equals("--kafka-url")) {
        kafkaUrl = args[++i];
      }
    }

    if (StringUtils.isEmpty(url) || StringUtils.isEmpty(user) || StringUtils.isEmpty(pass)
        || StringUtils.isEmpty(projectName)) {
      System.err.println(usage);
      System.exit(1);
    }

    // get KafkaProducer
    final LabkeyKafkaProducer lp = new LabkeyKafkaProducer(new URL(url), user, pass, kafkaTopic, kafkaUrl);

    // adding shutdown hook for shutdown gracefully
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
    {
      public void run() {
        System.out.println();
        System.out.format("[%s] Exiting app.\n", lp.getClass().getSimpleName());
        lp.closeProducer();
      }
    }));

    // get new studies
    while (lp.isRunning()) {
      // TODO we are querying for all of them, and publishing new ones, but updates are not handled
      // TODO Option1: From time to time query everything, and check if any of them have been updated
      // TODO Option2: Get a listener to the labkey system and pull only when changes were done (best option)
      List<Map<String, Object>> newStudies = lp.getNewStudies(projectName);
      // send new studies to kafka
      lp.sendStudies(newStudies);
      // waiting
      System.out.format("[%s] Waiting for %d seconds \n", lp.getClass().getSimpleName(), waitTime);
      lp.politeWait(waitTime * 1000);
    }
  }

  /**
   * Send labKey studies into kafka
   * @param newStudies
   */
  public void sendStudies(List<Map<String, Object>> newStudies) {
    for(Map<String, Object> row : newStudies) {
      this.sendKafka(generateStudyJSON(row).toJSONString());
      System.out.format("[%s] Sending to kafka: %s", this.getClass().getSimpleName(), generateStudyJSON(row).toJSONString());
    }
  }

  /**
   * Gets new studies from a specific project name.
   * @param projectName
   * @return
   * @throws IOException
   * @throws CommandException
   */
  private List<Map<String, Object>> getNewStudies(String projectName) throws IOException,
  CommandException {
    List<Map<String, Object>> projectStudies = getAllProjectStudies(projectName);
    List<Map<String, Object>> newStudies = new ArrayList<Map<String, Object>>();

    for (Map<String, Object> row : projectStudies) {
      Object rowUuid = row.get("container");
      // if not seen before then, add as previous
      if (!PREV_SEEN.contains(UUID.fromString(rowUuid.toString()))) {
        PREV_SEEN.add(UUID.fromString(rowUuid.toString()));
        newStudies.add(row);
      }
    }
    return newStudies;
  }

  /**
   * Gets studies from a specific project
   */
  public List<Map<String, Object>> getAllProjectStudies(String projectName) throws IOException,
  CommandException {
    // create a SelectRowsCommand to call the selectRows.api
    SelectRowsCommand cmd = new SelectRowsCommand("study", "Study");
    cmd.setContainerFilter(ContainerFilter.CurrentAndSubfolders);
    // execute the command against the connection
    // within the Api Test project folder
    SelectRowsResponse resp = cmd.execute(this.connection, TOP_PROJECT);
    //System.out.println(resp.getRowCount() + " rows were returned.");
    return resp.getRows();
  }

  /**
   * Generate JSON object
   *
   * @param studyData
   * @return
   */
  public JSONArray generateStudiesJSON(List<Map<String, Object>> studyData) {
    JSONArray jsonObj = new JSONArray();
    // loop over the returned rows
    for (int i = 0; i < studyData.size(); i++) {
      Map<String, Object> study = studyData.get(i);
      jsonObj.add(generateStudyJSON(study));
    }
    return jsonObj;
  }

  /**
   * Generates a JSON object from a study
   * @param study
   * @return
   */
  public JSONObject generateStudyJSON(Map<String, Object> study) {
    JSONObject jsonObj = new JSONObject();
    jsonObj.put(Constants.SOURCE_TAG, LABKEY_SOURCE_VAL);
    String[] keySet = study.keySet().toArray(new String[]{});
    for (int j = 0; j < keySet.length; j++) {
      String key = keySet[j];
      Object val = StringEscapeUtils.escapeJson(String.valueOf(study.get(keySet[j])));
      if (key.equals("Container")) {
          key = "id";
      } else if (key.equals("Description")) {
        key = "Study_Description";
      } else if (key.equals("_labkeyurl_Container")) {
        key = "Labkeyurl_Container";
      } else if (key.equals("_labkeyurl_Label")) {
        key = "Labkeyurl_Label";
      } else if (key.equals("Grant")) {
        key = "Study_Grant_Number";
      } else if (key.equals("Investigator")) {
        val = JSONValue.toJSONString(Lists.newArrayList(StringUtils.split(val.toString().replace(" ","").replace(",","-"), ";")).toString());
      } else {
        key = StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(key),"_");
      }
      jsonObj.put(key, val);
    }
    return jsonObj;
  }
}

