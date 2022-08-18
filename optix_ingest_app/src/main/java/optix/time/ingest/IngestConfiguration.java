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

import optix.time.ingest.sources.IngestSource;
import optix.time.ingest.transforms.RecordTransformer;
import optix.time.ingest.writers.IngestWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * IngestConfiguration is a Singleton class responsible for all inter-application configuration.
 * From this configuration, new instances of sources and writers can be created.
 */
public final class IngestConfiguration {

    final static Log logger = LogFactory.getLog(IngestConfiguration.class);

    private int targetThreads;
    private String name;

    //these could be maps for some speedup of access - likely small size though anyway
    private final List<SourceConfiguration> sources=new ArrayList<>();
    private final List<WriterConfiguration> writers=new ArrayList<>();
    private final List<TypeConfiguration> types=new ArrayList<>();

    private boolean initialized=false;
    private boolean loadConfigErrors=false;
    private static IngestConfiguration instance=new IngestConfiguration();


    /**
     * Get the TargetThreads defined from the local configuration
     * @return int - number of target threads for use with the executor service
     */
    public int getTargetThreads(){return this.targetThreads;}

    /**
     * Get the unique application deployment name specified from the local config
     * @return String - unique application deployment name
     */
    public String getName(){
        return this.name;
    }

    /**
     * List of sources with their respective configuration items
     * @return
     */
    public Iterable<SourceConfiguration> getSources() {
        return this.sources;
    }

    /**
     * Check if the current source configuration list contains the specified source name
     * @param name - source name to check
     * @return true if source name is found within sources list.
     */
    public boolean hasSource(String name){
        for(SourceConfiguration cur:sources){
            if (cur.name.equals(name))
                return true;
        }
        return false;
    }

    /**
     * Retrieve the specified source from the sources list
     * @param name - source name to retrieve
     * @return SourceConfiguration entry, null if not found
     */
    public SourceConfiguration getSource(String name){
        for(SourceConfiguration cur:sources){
            if (cur.name.equals(name)){
                return cur;
            }
        }
        return null;
    }

    /**
     * List of writers with their respective configuration items
     * @return
     */
    public Iterable<WriterConfiguration> getWriters() {
        return this.writers;
    }

    /**
     * Check if the current writer configuration list contains the specified writer name
     * @param name - writer name to check
     * @return true if writer name is found within writers list.
     */
    public boolean hasWriter(String name){
        for(WriterConfiguration cur:writers){
            if (cur.name.equals(name))
                return true;
        }
        return false;
    }

    /**
     * Retrieve the specified writer from the writers list
     * @param name - writer name to retrieve
     * @return WriterConfiguration entry, null if not found
     */
    public WriterConfiguration getWriter(String name){
        for(WriterConfiguration cur:writers){
            if (cur.name.equals(name)){
                return cur;
            }
        }
        return null;
    }

    /**
     * List of types
     * @return
     */
    public Iterable<TypeConfiguration> getTypes() {return this.types;}

    /**
     * Check if the specified type exists within the types list
     * @param name - type name to retrieve
     * @return true if type name is found
     */
    public boolean hasType(String name){
        for(TypeConfiguration cur:types){
            if (cur.name.equals(name))
                return true;
        }
        return false;
    }

    /**
     * Retrieve the TypeConfiguration
     * @param name - name of the type config
     * @param cat - TypeCategory which specifies whether this type config is a source, writer, or transformer
     * @return TypeConfiguration element if found
     */
    public TypeConfiguration getType(String name, TypeCategory cat){
        for(TypeConfiguration cur:types){
            if (cur.name.equals(name) && cur.category == cat){
                return cur;
            }
        }
        return null;
    }

    /**
     * Initialize the IngestConfiguration. Loads the local configuration with LocalConfig and generates configuration
     * lists for sources, writers, and types.
     *
     * Source and Writer objects are not created here, this is handled in the IngestEngine.
     *
     * @return true if initialization succeeds
     */
    public boolean initialize(){
        if (!this.initialized){
            //do initialization
            logger.info("Initializing optix.time.ingest.IngestConfiguration");

            //Clear previously set values
            this.types.clear();
            this.writers.clear();
            this.sources.clear();

            //Initialize types, writers, and sources from local config values
            //Check for system property defining config file location
            String configFileLocation = System.getProperty("configFile");
            if(configFileLocation == null){
                configFileLocation = "config.json";
            }
            LocalConfig localConfig = new LocalConfig(configFileLocation);
            this.name = localConfig.getDeploymentName();

            this.targetThreads = localConfig.getTargetThreads();

            //Types
            localConfig.getSourceTypes().forEach((name, typeName)-> {
                TypeConfiguration t = new TypeConfiguration(TypeCategory.DataSource, name, typeName);
                if (t != null && t.hasClass()) {
                    this.types.add(t);
                } else {
                    logger.error("failed to load a type: " + name + ", class: " + typeName);
                    loadConfigErrors = true;
                }
            });
            localConfig.getWriterTypes().forEach((name, typeName)-> {
                TypeConfiguration t = new TypeConfiguration(TypeCategory.Writer, name, typeName);
                if (t != null && t.hasClass()) {
                    this.types.add(t);
                } else {
                    logger.error("failed to load a type: " + name + ", class: " + typeName);
                    loadConfigErrors = true;
                }
            });
            localConfig.getTransformerTypes().forEach((name, typeName)-> {
                TypeConfiguration t = new TypeConfiguration(TypeCategory.Transformer, name, typeName);
                if (t != null && t.hasClass()) {
                    this.types.add(t);
                } else {
                    logger.error("failed to load a type: " + name + ", class: " + typeName);
                    loadConfigErrors = true;
                }
            });

            //Sources
            localConfig.getSources().forEach((name, configItems)-> {
                String typeName = configItems.get("type");
                TypeConfiguration thisSourceType = getType(typeName, TypeCategory.DataSource);

                SourceConfiguration c=new SourceConfiguration(name, thisSourceType.getName()); //Configures a single data source entity w/batchsize and transformation
                if (c!=null && this.hasType(typeName)) {
                    if (thisSourceType.category == TypeCategory.DataSource) {
                        c.keys.putAll(configItems); //write all config keys from the config for this source to pass through
                        this.sources.add(c);
                    }else{
                        logger.error("failed to load a source (wrong type): "+typeName+", class: "+thisSourceType.getClassName());
                        loadConfigErrors = true;
                    }
                }else{
                    logger.error("failed to load a source: "+name+", class: "+thisSourceType.getClassName());
                    loadConfigErrors = true;
                }
            });


            //Writers
            localConfig.getWriters().forEach((name, configItems)-> {
                try {
                    String typeName = configItems.get("type");
                    TypeConfiguration thisWriterType = getType(typeName, TypeCategory.Writer);

                    WriterConfiguration w = new WriterConfiguration(name, thisWriterType.getName());
                    if (w != null && this.hasType(typeName)) {
                        if (thisWriterType.category == TypeCategory.Writer) {
                            w.keys.putAll(configItems); //write all config keys from the config for this writer to pass through
                            this.writers.add(w);
                        } else {
                            logger.error("failed to load a writer (wrong type): " + typeName + ", class: " + thisWriterType.getClassName());
                            loadConfigErrors = true;
                        }
                    } else {
                        logger.error("failed to load a writer: " + name + ", class: " + thisWriterType.getClassName());
                        loadConfigErrors = true;
                    }
                }catch (Exception e){
                    logger.error("failed to process an entry: " + name);
                }
            });

            if(!loadConfigErrors){
                logger.info("optix.time.ingest.IngestConfiguration initialized");
                this.initialized = true;
            }

            //do the actual initialization of sources/writers in the IngestEngine
        }
        return this.initialized;
    }

    /**
     * Gets the singular Singleton instance of the IngestConfiguration
     * @return
     */
    public static IngestConfiguration getInstance(){
        instance.initialize(); //safe to call over and over and over and over
        return instance;
    }

    private IngestConfiguration(){}

    /**
     * A SourceConfiguration object exists for each source specified in the local config. This class should be the sole
     * resource for inspecting source configuration within a running GIA.
     */
    public final class SourceConfiguration{
        private final String name;
        private final String type;
        private final HashMap<String, String> keys=new HashMap<>();

        /**
         * Get this source's name
         * @return source name
         */
        public String getName(){return this.name;}

        /**
         * Get this source's type
         * @return source type name
         */
        public String getType(){return this.type;}

        /**
         * Get a key pair configuration set
         * @return Set of all key names
         */
        public Set<String> getKeys(){return this.keys.keySet();}

        /**
         * Check to see if a particular config key exists for this source
         * @param key - key to check
         * @return true if key exists within configuration keys map
         */
        public boolean containsKey(String key){
            return this.keys.containsKey(key);
        }

        /**
         * Retrieve the value for a particular configuration key
         * @param key - key within the config entry
         * @return config value associated with the given key
         */
        public String get(String key){
            return this.keys.get(key);
        }

        /**
         * Create a new SourceConfiguration
         * @param name - name of the source
         * @param type - name of the associated source type
         */
        private SourceConfiguration(String name, String type){
            if (name!=null)
                this.name = name;
            else
                this.name = "";

            if (type!=null)
                this.type=type;
            else
                this.type = "";
        }
    }

    /**
     * A WriterConfiguration object exists for each writer specified in the local config. This class should be the sole
     * resource for inspecting writer configuration within a running GIA.
     */
    public final class WriterConfiguration{
        private final String name;
        private final String type;
        private final HashMap<String, String> keys=new HashMap<>();

        /**
         * Get this writer's name
         * @return writer name
         */
        public String getName(){return this.name;}

        /**
         * Get this writer's type
         * @return writer type name
         */
        public String getType(){return this.type;}

        /**
         * Get a key pair configuration set
         * @return Set of all key names
         */
        public Set<String> getKeys(){return this.keys.keySet();}

        /**
         * Check to see if a particular config key exists for this writer
         * @param key - key to check
         * @return true if key exists within configuration keys map
         */
        public boolean containsKey(String key){
            return this.keys.containsKey(key);
        }

        /**
         * Retrieve the value for a particular configuration key
         * @param key - key within the config entry
         * @return config value associated with the given key
         */
        public String get(String key){
            return this.keys.get(key);
        }

        /**
         * Create a new WriterConfiguration
         * @param name - name of the writer
         * @param type - name of the associated writer type
         */
        private WriterConfiguration(String name, String type){
            if (name!=null)
                this.name = name;
            else
                this.name = "";

            if (type!=null)
                this.type=type;
            else
                this.type = "";
        }
    }

    /**
     * TypeCategory is used to differentiate between types of source, writer, and transformer
     */
    public enum TypeCategory{
        DataSource,
        Transformer,
        Writer
    }

    /**
     * A TypeConfiguration object exists for each type specified in the local config. This class should be the sole
     * resource for inspecting type configuration and creating new instances of source/writer/transformer within a running GIA.
     */
    public final class TypeConfiguration{
        private final TypeCategory category;
        private final String name;
        private final String typeName;

        /**
         * Get the TypeCategory assigned to this type
         * @return TypeCategory of either DataSource, Transformer, or Writer
         */
        public TypeCategory getCategory(){return this.category;}

        /**
         * @return the name of this type
         */
        public String getName(){return this.name;}

        /**
         * @return the classname for this type
         */
        public String getClassName(){return this.typeName;}

        /**
         * Create a new TypeConfiguration
         * @param cat - TypeCategory for this type
         * @param name - name of the type
         * @param typeName - classname associated with this type. Will be used to create new instances via reflection
         */
        private TypeConfiguration(TypeCategory cat, String name, String typeName){
            this.category=cat;

            if (name!=null)
                this.name = name;
            else
                this.name = "";

            if (typeName!=null)
                this.typeName=typeName;
            else
                this.typeName = "";
        }

        /**
         * Check to see if the defined classname for this type is defined.
         * @return true if classname is set
         */
        public boolean hasClass(){
            try{
                return Class.forName(this.typeName)!=null;
            }catch (Exception e){ logger.error("", e); }
            return false;
        }

        /**
         * If this type is of TypeCategory.Writer, attempt to create a new IngestWriter instance via the configured
         * classname.
         * @return IngestWriter if new instance creation is successful, null otherwise
         */
        public IngestWriter createIngestWriter(){
            try{
                if (this.category == TypeCategory.Writer){
                    Class c = Class.forName(this.typeName);
                    if (c != null)
                        return (IngestWriter) (c.newInstance());
                }
            }catch (Exception e){ logger.error("", e); }

            return null;
        }

        /**
         * If this type is of TypeCategory.DataSource, attempt to create a new IngestSource instance via the configured
         * classname.
         * @return IngestSource if new instance creation is successful, null otherwise
         */
        public IngestSource createIngestSource(){
            try {
                if (this.category == TypeCategory.DataSource) {
                    Class c = Class.forName(this.typeName);
                    if (c != null)
                        return (IngestSource) (c.newInstance());
                }
            }catch (Exception e){ logger.error("", e); }

            return null;
        }

        /**
         * If this type is of TypeCategory.Transformer, attempt to create a new RecordTransformer instance via the configured
         * classname.
         * @return RecordTransformer if new instance creation is successful, null otherwise
         */
        public <F, T> RecordTransformer<F, T> createRecordTransformer(){
            try {
                if (this.category == TypeCategory.Transformer) {
                    Class c = Class.forName(this.typeName);
                    if (c != null)
                        return (RecordTransformer<F, T>) (c.newInstance());
                }
            }catch (Exception e){ logger.error("", e); }

            return null;
        }
    }
}
