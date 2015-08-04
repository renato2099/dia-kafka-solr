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
package org.dia.kafka.isatools.producer;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dia.kafka.AbstractDiaKafkaProducer;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.isatab.ISArchiveParser;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.dia.kafka.solr.consumer.SolrKafkaConsumer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import static org.dia.kafka.Constants.*;

public class ISAToolsKafkaProducer extends AbstractDiaKafkaProducer {

    /**
     * Tag for specifying things coming out of LABKEY
     */
    public final static String ISATOOLS_SOURCE_VAL = "ISATOOLS";
    /**
     * ISA files default prefix
     */
    private static final String DEFAULT_ISA_FILE_PREFIX = "s_";
    /**
     * Json jsonParser to decode TIKA responses
     */
    private static JSONParser jsonParser = new JSONParser();
    ;

    /**
     * Constructor
     */
    public ISAToolsKafkaProducer(String kafkaTopic, String kafkaUrl) {
        initializeKafkaProducer(kafkaTopic, kafkaUrl);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        String isaToolsDir = null;
        long waitTime = DEFAULT_WAIT;
        String kafkaTopic = TOPIC;
        String kafkaUrl = KAFKA_URL;

        // TODO Implement commons-cli
        String usage = "java -jar ./target/isatools-producer.jar [--tikaRESTURL <url>] [--isaToolsDir <dir>] [--wait <secs>] [--kafka-topic <topic_name>] [--kafka-url]\n";

        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--isaToolsDir")) {
                isaToolsDir = args[++i];
            } else if (args[i].equals("--kafka-topic")) {
                kafkaTopic = args[++i];
            } else if (args[i].equals("--kafka-url")) {
                kafkaUrl = args[++i];
            }
        }

        // Checking for required parameters
        if (StringUtils.isEmpty(isaToolsDir)) {
            System.err.format("[%s] A folder containing ISA files should be specified.\n", ISAToolsKafkaProducer.class.getSimpleName());
            System.err.println(usage);
            System.exit(0);
        }

        // get KafkaProducer
        final ISAToolsKafkaProducer isatProd = new ISAToolsKafkaProducer(kafkaTopic, kafkaUrl);
        DirWatcher dw = new DirWatcher(Paths.get(isaToolsDir));

        // adding shutdown hook for shutdown gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                System.out.println();
                System.out.format("[%s] Exiting app.\n", isatProd.getClass().getSimpleName());
                isatProd.closeProducer();
            }
        }));

        // get initial ISATools files
        List<JSONObject> newISAUpdates = isatProd.initialFileLoad(isaToolsDir);
        // send new studies to kafka
        isatProd.sendISAToolsUpdates(newISAUpdates);
        dw.processEvents(isatProd);

    }

    /**
     * Checks for files inside a folder
     *
     * @param innerFolder
     * @return
     */
    public static List<String> getFolderFiles(File innerFolder) {
        List<String> folderFiles = new ArrayList<String>();
        String[] innerFiles = innerFolder.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.startsWith(DEFAULT_ISA_FILE_PREFIX)) {
                    return true;
                }
                return false;
            }
        });

        for (String innerFile : innerFiles) {
            File tmpDir = new File(innerFolder.getAbsolutePath() + File.separator + innerFile);
            if (!tmpDir.isDirectory()) {
                folderFiles.add(tmpDir.getAbsolutePath());
            }
        }
        return folderFiles;
    }

    /**
     * Performs the parsing request to Tika
     *
     * @param files
     * @return
     */
    public static List<JSONObject> doTikaRequest(List<String> files) {
        List<JSONObject> jsonObjs = new ArrayList<JSONObject>();

        try {
            Parser parser = new ISArchiveParser();
            StringWriter strWriter = new StringWriter();

            for (String file : files) {
                JSONObject jsonObject = new JSONObject();

                // get metadata from tika
                InputStream stream = TikaInputStream.get(new File(file));
                ContentHandler handler = new ToHTMLContentHandler();
                Metadata metadata = new Metadata();
                ParseContext context = new ParseContext();
                parser.parse(stream, handler, metadata, context);

                // get json object
                jsonObject.put(SOURCE_TAG, ISATOOLS_SOURCE_VAL);
                JsonMetadata.toJson(metadata, strWriter);
                jsonObject = adjustUnifiedSchema((JSONObject) jsonParser.parse(new String(strWriter.toString())));
                //TODO Tika parsed content is not used needed for now
                //jsonObject.put(X_TIKA_CONTENT, handler.toString());
                System.out.format("[%s] Tika message: %s \n", ISAToolsKafkaProducer.class.getSimpleName(), jsonObject.toJSONString());

                jsonObjs.add(jsonObject);

                strWriter.getBuffer().setLength(0);
            }
            strWriter.flush();
            strWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        }
        return jsonObjs;
    }

    private static JSONObject adjustUnifiedSchema(JSONObject parse) {
        JSONObject jsonObject = new JSONObject();
        List invNames = new ArrayList<String>();
        List invMid = new ArrayList<String>();
        List invLastNames = new ArrayList<String>();

        Set<Map.Entry> set = parse.entrySet();
        for (Map.Entry entry : set) {
            String jsonKey = SolrKafkaConsumer.updateCommentPreffix(entry.getKey().toString());
            String solrKey = ISA_SOLR.get(jsonKey);

//            System.out.println("solrKey " + solrKey);
            if (solrKey != null) {
//                System.out.println("jsonKey: " + jsonKey + " -> solrKey: " + solrKey);
                if (jsonKey.equals("Study_Person_First_Name")) {
                    invNames.addAll(((JSONArray) JSONValue.parse(entry.getValue().toString())));
                } else if (jsonKey.equals("Study_Person_Mid_Initials")) {
                    invMid.addAll(((JSONArray) JSONValue.parse(entry.getValue().toString())));
                } else if (jsonKey.equals("Study_Person_Last_Name")) {
                    invLastNames.addAll(((JSONArray) JSONValue.parse(entry.getValue().toString())));
                }
                jsonKey = solrKey;
            } else {
                jsonKey = jsonKey.replace(" ", "_");
            }
            jsonObject.put(jsonKey, entry.getValue());
        }

        JSONArray jsonArray = new JSONArray();

        for (int cnt = 0; cnt < invLastNames.size(); cnt++) {
            StringBuilder sb = new StringBuilder();
            if (!StringUtils.isEmpty(invNames.get(cnt).toString()))
                sb.append(invNames.get(cnt)).append(" ");
            if (!StringUtils.isEmpty(invMid.get(cnt).toString()))
                sb.append(invMid.get(cnt)).append(" ");
            if (!StringUtils.isEmpty(invLastNames.get(cnt).toString()))
                sb.append(invLastNames.get(cnt));
            jsonArray.add(sb.toString());
        }
        if (!jsonArray.isEmpty()) {
            jsonObject.put("Investigator", jsonArray.toJSONString());
        }
        return jsonObject;
    }

    /**
     * Send message from IsaTools to kafka
     *
     * @param newISAUpdates
     */
    void sendISAToolsUpdates(List<JSONObject> newISAUpdates) {
        for (JSONObject row : newISAUpdates) {
            row.put(SOURCE_TAG, ISATOOLS_SOURCE_VAL);
            this.sendKafka(row.toJSONString());
            System.out.format("[%s] New message posted to kafka.\n", this.getClass().getSimpleName());
        }
    }

    /**
     * Gets the ISATools updates from a directory
     *
     * @param isaToolsTopDir
     * @return
     */
    private List<JSONObject> initialFileLoad(String isaToolsTopDir) {
        System.out.format("[%s] Checking in %s\n", this.getClass().getSimpleName(), isaToolsTopDir);
        List<JSONObject> jsonParsedResults = new ArrayList<JSONObject>();
        List<File> innerFolders = getInnerFolders(isaToolsTopDir);

        for (File innerFolder : innerFolders) {
            jsonParsedResults.addAll(doTikaRequest(getFolderFiles(innerFolder)));
        }

        return jsonParsedResults;
    }

    /**
     * Gets the inner folders inside a folder
     *
     * @param isaToolsTopDir
     * @return
     */
    private List<File> getInnerFolders(String isaToolsTopDir) {
        List<File> innerFolders = new ArrayList<File>();
        File topDir = new File(isaToolsTopDir);
        String[] innerFiles = topDir.list();
        for (String innerFile : innerFiles) {
            File tmpDir = new File(isaToolsTopDir + File.separator + innerFile);
            if (tmpDir.isDirectory()) {
                innerFolders.add(tmpDir);
            }
        }
        return innerFolders;
    }
}
