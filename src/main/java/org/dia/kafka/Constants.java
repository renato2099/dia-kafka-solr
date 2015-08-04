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

//import com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableMap;

import java.util.*;

/**
 * Class containing constants for producing and consuming msgs to/from Kafka.
 */
public class Constants {
    /**
     * Default waiting time between calls is 30 secs.
     */
    public static final long DEFAULT_WAIT = 30;
    /**
     * Tag to be used for specifying data source
     */
    public final static String SOURCE_TAG = "DataSource";
    /**
     * Default Solr collection
     */
    public static final String DEFAULT_SOLR_COL = "dia-solr-kafka";
    /**
     * Fields ignored for OODT messages
     */
    public static final String[] OODT_IGNORED_FIELDS = new String[]{"ProductReceivedTime", "ProductName",
            "ProductId", "ProductStructure", "DataVersion", "CAS.ProductName", "CAS.ProductReceivedTime",
            "CAS.ProductId"};
    public static final Set<String> OODT_IGNORED_SET = new HashSet<String>(Arrays.asList(OODT_IGNORED_FIELDS));
    /**
     * Fields ignored for ISATools messages
     */
    public static final String[] ISA_IGNORED_FIELDS = new String[]{"Data_Asset"};
    public static final Set<String> ISA_IGNORED_SET = new HashSet<String>(Arrays.asList(ISA_IGNORED_FIELDS));

    /**
     * Field mapping between ISATools and Solr
     * TODO maybe there is a better way on doing this, but right now the problem is the imposed order
     * between first_names, mid_initials and last_names to create the investigator name.
     */
    public static final Map<String, String> ISA_SOLR = ImmutableMap.<String, String>builder()
            .put("DA_Number", "id")
            .put("Study_Person_First_Name", "Investigator")
            .put("Study_Person_Mid_Initials", "Investigator")
            .put("Study_Person_Last_Name", "Investigator")
            .put("Tags", "Study_Tags")
            .put("Created_with_configuration", "Created_With_Configuration")
            .build();





    /**
     * Mappings between OODT and LabKey to ISATools fields
     */
    public static final Map<String, String> OODT_ISA = null;
//    = ImmutableMap.<String, String>builder()
//            .put("DataAssetGroupNumber", "DA_Number")
//            .put("ProjectDescription", "Study_Description")
//            .put("ProjectName", "Study_Title")
//            .put("Investigator", "")
//            .put("ProductType", "")
//            .put("FileLocation", "")
//            .put("MimeType", "")
//            .put("DataAssetGroupName", "")
//            .put("Investigator", "")
//            .put("AssayPurpose", "")
//            .put("TargetName", "")
//            .put("AssetType", "")
//            .put("SampleType", "")
//            .put("Vendor", "")
//            .put("Platform", "")
//            .put("Department", "Department")
//            .put("Compound", "Celgene_Compound")
//            .put("Filename", "Study_File_Name").build();

    public static final Map<String, String> LABKEY_ISA = null;
//    ImmutableMap.<String, String>builder()
//            .put("DA_Number", "DA_Number")
//            .put("Participant_Alias_Source_Property", "")
//            .put("Participant_Alias_Property", "")
//            .put("End_Date", "")
//            .put("_labkeyurl_Label", "")
//            .put("Assay_Plan", "")
//            .put("Start_Date", "")
//            .put("Investigator", "")
//            .put("Participant_Alias_Dataset_Id", "")
//            .put("Species", "")
//            .put("_labkeyurl_Container", "")
//            .put("Description", "Study_Description")
//            .put("Grant", "Study_Grant_Number")
//            .put("Label", "Study_Tags")
//            .put("Compound", "Celgene_Compound")
//            .put("Filename", "Study_File_Name").build();

    public static final Map<String, String> LABKEY_OODT = null;
//    ImmutableMap.<String, String>builder()
//            .put("DA_Number", "DataAssetGroupNumber")
//            .put("Participant_Alias_Source_Property", "")
//            .put("Participant_Alias_Property", "")
//            .put("End_Date", "")
//            .put("Labkeyurl_Label", "")
//            .put("Assay_Plan", "")
//            .put("Start_Date", "")
//            .put("Investigator", "")
//            .put("Participant_Alias_Dataset_Id", "")
//            .put("Species", "")
//            .put("Labkeyurl_Container", "FileLocation")
//            .put("Description", "ProjectName")
//            .put("Description", "ProjectDescription")
//            .put("Grant", "")
//            .put("Label", "")
//            .put("Compound", "")
//            .put("Filename", "")
//            .build();

    public static final Map<String, String> OODT_LABKEY = null;
//    ImmutableMap.<String, String>builder()
//            .put("DataAssetGroupNumber", "DA_Number")
//            .put("ProjectDescription", "Description")
//            .put("ProjectName", "Description")
//            .put("Investigator", "Investigator")
//            .put("ProductType", "")
//            .put("FileLocation", "Labkeyurl_Container")
//            .put("MimeType", "")
//            .put("DataAssetGroupName", "")
//            .put("Investigator", "")
//            .put("AssayPurpose", "")
//            .put("TargetName", "")
//            .put("AssetType", "")
//            .put("SampleType", "")
//            .put("Vendor", "")
//            .put("Platform", "")
//            .put("Department", "")
//            .put("Compound", "")
//            .put("Filename", "")
//            .build();

    public static final Map<String, String> ISA_LABKEY = null;
//    ImmutableMap.<String, String>builder()
//            .put("DA_Number", "DA_Number")
//            .put("", "Participant_Alias_Source_Property")
//            .put("", "Participant_Alias_Property")
//            .put("", "End_Date")
//            .put("", "Labkeyurl_Label")
//            .put("", "Assay_Plan")
//            .put("", "Start_Date")
//            .put("", "Investigator")
//            .put("", "Participant_Alias_Dataset_Id")
//            .put("", "Species")
//            .put("", "Labkeyurl_Container")
//            .put("Study_Description", "Description")
//            .put("Study_Grant_Number", "Grant")
//            .put("Study_Tags", "Label")
//            .put("", "Compound")
//            .put("", "Filename")
//            .build();
    /**
     * Topic name
     */
    public static final String KAFKA_TOPIC = "dia-updates";
    /**
     * Kafka url
     */
    public static final String KAFKA_URL = "localhost:9092";
    /**
     * Kafka serializer class
     */
    public static final String SERIALIZER = "kafka.serializer.StringEncoder";
    /**
     * Kafka GroupId
     */
    public static String GROUP_ID = "dia-group";
    /**
     * Kafka url
     */
    public static final String ZOO_URL = "localhost:2181";
    /**
     * Solr url
     */
    public static final String SOLR_URL = "http://localhost:8983/solr/";

    /**
     * Data sources tag values
     */
    public enum DataSource {
        OODT("oodt"), ISATOOLS("isatools"), LABKEY("labkey");

        private String value;

        DataSource(String v) {
            this.value = v;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }
//            .put(2, "two")
//                    // ...
//            .put(15, "fifteen")
//            .build();
//    public static final Map<Integer, String> LABKEY_ISA = ImmutableMap.<Integer, String>builder()
//            .put(1, "one")
//            .put(2, "two")
//                    // ...
//            .put(15, "fifteen")
//            .build();
}
