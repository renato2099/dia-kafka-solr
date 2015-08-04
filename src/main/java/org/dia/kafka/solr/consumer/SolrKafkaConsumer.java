/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.kafka.solr.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.dia.kafka.AbstractDiaKafkaConsumer;
import org.dia.kafka.Constants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.dia.kafka.Constants.*;

/**
 * Consumes Kafka data and persists it into Solr.
 */
public class SolrKafkaConsumer extends AbstractDiaKafkaConsumer {

    /**
     * Logger
     */
    public static final Logger LOG = LoggerFactory
            .getLogger(SolrKafkaConsumer.class);

    /**
     * Solr server
     */
    private SolrServer solrServer;

    /**
     * Constructor
     *
     * @param solrUrl
     * @param zooUrl
     * @param topic
     */
    public SolrKafkaConsumer(String solrUrl, String solrCollection, String zooUrl, String topic) {
        initialize(solrUrl, solrCollection, zooUrl, topic);
        this.solrServer = new HttpSolrServer(solrUrl.concat(solrCollection));
        System.out.format("[%s] pushing data into %s\n", this.getClass().getSimpleName(), Constants.SOLR_URL.concat(solrCollection));
    }

    /**
     * Default constructor
     */
    public SolrKafkaConsumer() {
        initialize();
        this.solrServer = new HttpSolrServer(SOLR_URL.concat(DEFAULT_SOLR_COL));
        LOG.debug("[%s] pushing data into %s", this.getClass().getSimpleName(), Constants.SOLR_URL.concat(DEFAULT_SOLR_COL));
    }

    public static void main(String[] args) {
        String solrCollection = DEFAULT_SOLR_COL;
        String solrUrl = SOLR_URL;
        String zooUrl = ZOO_URL;
        String kafkaTopic = TOPIC;

        // TODO Implement commons-cli
        String usage = "java -jar ./target/solr-consumer.jar [--solr-url <url>] [--sorl-collection <collection>] [--zoo-url <url>] " +
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

        final SolrKafkaConsumer solrK = new SolrKafkaConsumer(solrUrl, solrCollection, zooUrl, kafkaTopic);
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
    public void processMessage(String msg) {
        this.addMessageToSolr(msg);
        this.commitToSolr();
    }

    /**
     * Add decoded message into Solr.
     *
     * @param kafkaMsg
     */
    private void addMessageToSolr(String kafkaMsg) {
        System.out.format("[%s] Adding message to Solr\n", this.getClass().getSimpleName());
        JSONObject jsonObject = (JSONObject) JSONValue.parse(kafkaMsg);
        DataSource source = DataSource.valueOf(jsonObject.get(SOURCE_TAG).toString());
        SolrInputDocument doc = null;
        switch (source) {
            case OODT:
                doc = convertOodtMsgSolr(jsonObject);
                break;
            case ISATOOLS:
                doc = convertIsaMsgSolr(jsonObject);
                break;
            case LABKEY:
                doc = convertLabkeyMsgSolr(jsonObject);
                break;
            default:
                throw new IllegalArgumentException("Data source not supported yet, please try one "
                        + "of 'oodt', 'labkey' or 'isatools'. Additionally, please write to celgene-jpl@jpl.nasa.gov"
                        + "and let us know about new data sources you would like to see supported.");
        }
        try {
            this.solrServer.add(doc);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Converts ISATools messages into Solr documents
     *
     * @param jsonObject
     * @return
     */
    private SolrInputDocument convertIsaMsgSolr(JSONObject jsonObject) {
        System.out.format("[%s] Processing msg from %s\n", this.getClass().getSimpleName(), DataSource.ISATOOLS);
        final SolrInputDocument inputDoc = new SolrInputDocument();
        jsonObject.remove(SOURCE_TAG);
        Iterator<?> keys = jsonObject.keySet().iterator();
        List<String> invNames = new ArrayList<String>();
        List<String> invMid = new ArrayList<String>();
        List<String> invLastNames = new ArrayList<String>();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            String cleanKey = updateCommentPreffix(key);
            if (!ISA_IGNORED_SET.contains(cleanKey)) {
                if (cleanKey.equals("DA_Number")) {
                    inputDoc.addField("id", jsonObject.get(key));
                } else if (cleanKey.equals("Study_Person_First_Name")) { // dealing with investigators' names
                    invNames.add(jsonObject.get(key).toString());
                } else if (cleanKey.equals("Study_Person_Mid_Initials")) {
                    invMid.add(jsonObject.get(key).toString());
                } else if (cleanKey.equals("Description")) {
                    invLastNames.add(jsonObject.get(key).toString());
                } else if (cleanKey.equals("Tags")) {
                    inputDoc.addField("Study_Tags", jsonObject.get(key));
                } else if (cleanKey.equals("Created_with_configuration")) {
                    inputDoc.addField("Created_With_Configuration", jsonObject.get(key));
                } else {//if (!key.startsWith("_")){
                    inputDoc.addField(cleanKey, jsonObject.get(key));
                }
            }
        }
        JSONArray jsonArray = new JSONArray();
        for (int cnt = 0; cnt < invLastNames.size(); cnt++) {
            StringBuilder sb = new StringBuilder();
            sb.append(invNames.get(cnt) != null ? invNames.get(cnt) : "").append(" ");
            sb.append(invMid.get(cnt) != null ? invMid.get(cnt) : "").append(" ");
            sb.append(invLastNames.get(cnt) != null ? invLastNames.get(cnt) : "");
            jsonArray.add(sb.toString());
        }
        if (!jsonArray.isEmpty())
            inputDoc.addField("Investigator", jsonArray.toJSONString());
        return inputDoc;
    }

    /**
     * Deletes the 'Comment[...] preffix from JSON keys'
     *
     * @param key
     * @return
     */
    public static String updateCommentPreffix(String key) {
        String newKey = key;
        if (key.startsWith("Comment")) {
            newKey = key.replace("Comment", "").trim().replaceAll("\\[|\\]", "");
        }
        return newKey.replaceAll(" ", "_");
    }

    /**
     * Converts OODT messages into Solr documents
     * {@code {"TargetName":["target"],"FileLocation":["\/data\/archive\/035131.pdf"],"ProjectName":["project"],"DataAssetGroupName":["data asset"],"Compound":["compound"],"Investigator":["investigator"],"ProductStructure":["Flat"],"MimeType":["application\/octet-stream","application\/pdf","application","pdf"],"AssetType":["Processed"],"CAS.ProductName":["035131.pdf"],"CAS.ProductReceivedTime":["2015-06-09T08:31:11.749-07:00"],"Filename":["035131.pdf"],"DataSource":"OODT","SampleType":["type"],"AssayPurpose":["assay"],"Vendor":["vendor"],"CAS.ProductId":["8ed237e5-0ebc-11e5-8684-d7284d57b93d"],"ProductType":["Processed"],"ProjectDesciption":["description"],"ProductName":["DA0000023,Processed,035131.pdf"],"ExperimentType":["experiment1"],"DataAssetGroupNumber":["DA0000023"],"Department":["dep"],"Platform":["aplatform"]}}
     *
     * @param jsonObject
     * @return
     */
    private SolrInputDocument convertOodtMsgSolr(JSONObject jsonObject) {
        System.out.format("[%s] Processing msg from %s\n", this.getClass().getSimpleName(), DataSource.OODT);
        final SolrInputDocument inputDoc = new SolrInputDocument();
        jsonObject.remove(SOURCE_TAG);

        Iterator<?> keys = jsonObject.keySet().iterator();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            if (!Constants.OODT_IGNORED_SET.contains(key)) {
                if (key.equals("DataAssetGroupNumber")) {
                    inputDoc.addField("id", jsonObject.get(key));
                } else if (key.equals("ProjectDesciption")) {
                    inputDoc.addField("Study_Description", jsonObject.get(key));
                } else if (key.equals("ProjectName")) {
                    inputDoc.addField("Study_Title", jsonObject.get(key));
                } else {//if (!key.startsWith("_")){
                    inputDoc.addField(key, jsonObject.get(key));
                }
            }
        }
        return inputDoc;
    }

    /**
     * {@code
     * {"studies":[
     * {
     * "ParticipantAliasSourceProperty" : "null",
     * "ParticipantAliasProperty" : "null",
     * "Description" : "Some Description",
     * "EndDate" : "null",
     * "_labkeyurl_Label" : "/labkey/project/Project Name/start.view?",
     * "AssayPlan" : "null",
     * "Grant" : "Some Foundation",
     * "StartDate" : "Tue Jan 01 00:00:00 EST 2008",
     * "Investigator" : "Some PI, PhD",
     * "ParticipantAliasDatasetId" : "null",
     * "Label" : "Interactive Example - Study",
     * "Species" : "null",
     * "Container" : "6f743c1b-9506-1032-b9bf-51af32c4d069",
     * "_labkeyurl_Container" : "/labkey/project/Study Name/begin.view?",
     * }
     * ]
     * }
     * }
     *
     * @param jsonObject
     * @return
     * @throws IOException
     */
    private SolrInputDocument convertLabkeyMsgSolr(JSONObject jsonObject) {
        System.out.format("[%s] Processing msg from %s\n", this.getClass().getSimpleName(), DataSource.LABKEY);
        final SolrInputDocument inputDoc = new SolrInputDocument();
        jsonObject.remove(SOURCE_TAG);
        Iterator<?> keys = jsonObject.keySet().iterator();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            if (key.equals("Container")) {
                inputDoc.addField("id", jsonObject.get(key));
            } else if (key.equals("Description")) {
                inputDoc.addField("Study_Description", jsonObject.get(key));
            } else if (key.equals("_labkeyurl_Container")) {
                inputDoc.addField("Labkeyurl_Container", jsonObject.get(key));
            } else if (key.equals("_labkeyurl_Label")) {
                inputDoc.addField("Labkeyurl_Label", jsonObject.get(key));
            } else if (key.equals("Investigator")) {
                inputDoc.addField("Investigator", StringUtils.split(jsonObject.get(key).toString(), ";"));
            } else if (key.equals("Grant")) {
                inputDoc.addField("Study_Grant_Number", jsonObject.get(key));
            } else {
                inputDoc.addField(key, jsonObject.get(key).toString());
            }
        }

        return inputDoc;
    }

    /**
     * Commits documents into Solr.
     */
    public void commitToSolr() {
        try {
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
