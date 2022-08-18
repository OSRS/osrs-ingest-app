/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package optix.time.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.util.HashMap;

/**Config should have 4 primary areas
    DeployName -- single string parameter naming this deployment
    Sources -- collection of SourceConfigurations
    Writers -- collection of WriterConfigurations
    Types -- collection of TypeConfigurations grouped by purpose
    ---- DataSources (implement optix.time.ingest.sources.IngestSource)
    ---- Transformers (implement optix.time.ingest.transforms.RecordTransformer)
    ---- DataWriters (implement optix.time.ingest.writers.IngestWriter)
 **/
public class LocalConfig {

    final static Log logger = LogFactory.getLog(LocalConfig.class);

    public LocalConfig(String configFileName) {
        loadConfigFile(configFileName);
    }

    //Getters and config items

    private int targetThreads;
    /**
     * Get the 'TargetThreads' config item
     * @return TargetThreads - number of threads to use with the executor service.
     */
    public int getTargetThreads(){return this.targetThreads;}


    private String deployName = "";
    /**
     * Get the 'DeployName' config item
     * @return DeployName - unique name which identifies this GIA instance
     */
    public String getDeploymentName(){
        return deployName;
    }


    private final HashMap<String, HashMap<String, String>> sources = new HashMap<>();
    /**
     * Get sources with their config.
     * @return HashMap&lt;String, HashMap&lt;String, String&gt;&gt; Sources - Map of sources with their respective configuration
     */
    public HashMap<String, HashMap<String, String>> getSources(){
        return sources;
    }


    private final HashMap<String, HashMap<String, String>> writers = new HashMap<>();
    /**
     * Get writers with their config.
     * @return HashMap&lt;String, HashMap&lt;String, String&gt;&gt; Writers - Map of sources with their respective configuration
     */
    public HashMap<String, HashMap<String, String>> getWriters(){
        return writers;
    }


    private final HashMap<String, String> transformerTypes = new HashMap<>();
    /**
     * Get map of transformer types where key=typeName and value=ClassName
     * Used to load classes via reflection
     * @return HashMap&lt;String, String&gt; types - Map of types with class names
     */
    public HashMap<String, String> getTransformerTypes(){
        return transformerTypes;
    }


    private final HashMap<String, String> sourceTypes = new HashMap<>();
    /**
     * Get map of source types where key=typeName and value=ClassName
     * Used to load classes via reflection
     * @return HashMap&lt;String, String&gt; types - Map of types with class names
     */
    public HashMap<String, String> getSourceTypes(){
        return sourceTypes;
    }


    private final HashMap<String, String> writerTypes = new HashMap<>();
    /**
     * Get map of writer types where key=typeName and value=ClassName
     * Used to load classes via reflection
     * @return HashMap&lt;String, String&gt; types - Map of types with class names
     */
    public HashMap<String, String> getWriterTypes(){
        return writerTypes;
    }

    //Load a type (DataSources, DataWriters, or Transforms) and return a mapping of config items
    private boolean loadTypeConfig(JSONObject appConfig, String typeItem, HashMap<String, String> retMap){
        try {
            JSONObject typeObjects = (JSONObject) appConfig.get("Types");

            if (typeObjects!=null) {
                JSONObject sourceTypeObjects = (JSONObject) typeObjects.get(typeItem);
                if (sourceTypeObjects!=null) {
                    for (Object k : sourceTypeObjects.keySet()) {
                        String key = (String) k;
                        Object j=sourceTypeObjects.get(key);
                        if (j!=null)
                            retMap.put(key, j.toString());
                    }
                }
            }
            return true;
        }catch (Exception e){}
        return false;
    }

    //Generic loader to parse the config file for either Sources or Writers
    private boolean loadGenericConfig(JSONObject appConfig, String configToLoad, HashMap<String, HashMap<String, String>> retMap){
        JSONObject sourcesObjects = (JSONObject) appConfig.get(configToLoad);

        try {
            for (Object k : sourcesObjects.keySet()) {
                String key = (String) k;

                //disallow null or empty keys
                if (key!=null && key.length()>0) {
                    JSONObject configBlob = (JSONObject) sourcesObjects.get(key);
                    if (configBlob!=null) {
                        if (!retMap.containsKey(key)) {
                            retMap.put(key, new HashMap<>());
                        }
                        HashMap<String,String> map = retMap.get(key);

                        for (Object v : configBlob.keySet()) {
                            String configItem = (String) v;
                            map.put(configItem, configBlob.get(configItem).toString());
                        }
                    }
                }
            }
            return true;
        }catch (Exception e){
            logger.error("", e);
        }

        return false;
    }

    //Return Sources config item map
    private void loadSourcesConfig(JSONObject appConfig){
        loadGenericConfig(appConfig, "Sources", this.sources);
    }

    //Return Writers config item map
    private void loadWritersConfig(JSONObject appConfig){
        loadGenericConfig(appConfig, "Writers", this.writers);
    }

    //Parse config file and build mappings of content

    /**
     * Load the specified config file
     * @param configFileName fully qualified path of config file
     * @return Boolean - True if config file successfully loaded
     */
    private boolean loadConfigFile(String configFileName){

        logger.info("Loading LocalConfig");

        //Parse the config file and generate configuration objects
        JSONParser jsonParser = new JSONParser();

        //Reads the config from internal jar resources
        //InputStream configStream = getClass().getClassLoader().getResourceAsStream(configFileName);

        //Reads the config from local file to jar execution
        InputStream configStream = null;
        try {
            configStream = new FileInputStream(new File(configFileName));
        } catch (FileNotFoundException e) {
            logger.error("", e);
        }

        if(configStream == null){
            logger.error("File not loaded");
            return false;
        }

        try (InputStreamReader reader = new InputStreamReader(configStream)) {
            //Read JSON file
            Object configRoot = jsonParser.parse(reader);

            JSONObject appConfig = (JSONObject) configRoot;

            //Deployment Name
            this.deployName = appConfig.getOrDefault("DeployName", "").toString();

            //Number of worker threads to target for transformers
            try{
                String tthreads = appConfig.getOrDefault("TargetThreads", "").toString();
                this.targetThreads = Integer.parseInt(tthreads);
            }catch (Exception e){
                logger.error("", e);
            }

            if (this.targetThreads<1)
                this.targetThreads = 3;

            //Types Config
            loadTypeConfig(appConfig, "DataSources", this.sourceTypes);
            loadTypeConfig(appConfig, "DataWriters", this.writerTypes);
            loadTypeConfig(appConfig, "Transformers", this.transformerTypes);

            //Sources Config
            loadSourcesConfig(appConfig);

            //Writers Config
            loadWritersConfig(appConfig);

        } catch (Exception e) {
            logger.error("", e);
            return false;
        }

        return true;
    }
}
