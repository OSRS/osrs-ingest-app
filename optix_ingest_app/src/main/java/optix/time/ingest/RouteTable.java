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
import optix.time.ingest.writers.IngestWriter;
import optix.time.ingest.writers.TransformerWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implements the set of routes used by the IngestRouter for routing all messages.
 */
public class RouteTable implements Cloneable{

    final static Log logger = LogFactory.getLog(RouteTable.class);

    /*
    {s:{t:{(xform, w)}}}
    Where
        s = source name
        t = topic name
        xform = transform name, can be null if no transform is provided.
        w = the writer to handle this record

        xform + w are wrapped in a WriterHandler object
    */
    private HashMap<String, HashMap<String, WriterHandler>> routeTable;

    public RouteTable(){
        this.routeTable = new HashMap<>();
    }

    /**
     * Check if a particular source exists within a collection of metaEntries
     * @param metaEntries - metaEntries obtained from MetaRegistryLoader.fetchMetaEntries()
     * @param sourceToFind - The source to check existence
     * @return - True if source is found within the metaEntries collection
     */
    private boolean sourceExistsInMetaEntries(Iterable<MetaEntry>metaEntries, String sourceToFind){
        boolean found = false;

        for(MetaEntry me : metaEntries){
            if(me.sourceProvider.equals(sourceToFind)){
                found = true;
                break;
            }
        }

        return found;
    }

    /**
     * Check if a particular topic exists within a collection of metaEntries
     * @param metaEntries
     * @param sourceToFind - The source which is parent to the topicToFind
     * @param topicToFind - The topic to check existence
     * @return - True if topic is found within the metaEntries collection
     */
    private boolean topicExistsInMetaEntries(Iterable<MetaEntry>metaEntries, String sourceToFind, String topicToFind){
        boolean found = false;

        for(MetaEntry me : metaEntries){
            if(me.sourceProvider.equals(sourceToFind) &&
                    me.sourceTopic.equals(topicToFind)
            ){
                found = true;
                break;
            }
        }

        return found;
    }

    /**
     * Update the internal state of this route table given new MetaEntry information
     * @param metaEntries - metaEntries obtained from MetaRegistryLoader.fetchMetaEntries()
     */
    public void updateRoutes(Iterable<MetaEntry> metaEntries){

        IngestEngine engine = IngestEngine.getInstance();

        for(MetaEntry cur : metaEntries){
            logger.info("Attempting to create route for : " + cur);
            IngestSource src = engine.getSource(cur.sourceProvider);
            if (src!=null) {
                IngestWriter wr = engine.getWriter(cur.destProvider);
                if (wr!=null) {
                    if (cur.hasTransform()) {
                        IngestConfiguration.TypeConfiguration cat = engine.getType(cur.getTransformTypeName());
                        if (cat != null) {
                            TransformerWriter w = TransformerWriter.createTransform(cat, cur.getTransformMeta(), wr, cur.destProvider, cur.destTopic, cur.maxBatchSize);
                            updateRouteMap(cur.sourceProvider, cur.sourceTopic, cur.getTransformMeta(), w);

                            logger.info("Transform writer made for : " + cur );
                        }
                    }else{
                        //no-op writer, put in map
                        updateRouteMap(cur.sourceProvider, cur.sourceTopic, "", TransformerWriter.createTransform(null, null, wr, cur.destProvider, cur.destTopic, cur.maxBatchSize));
                        logger.info("No-Op detected for : " + cur );
                    }
                }
            }
        }

        pruneRouteMap(metaEntries);

    }

    /**
     * Insert a new route into the routeTable, if the entry already exists update with newer values.
     * @param sourceName - Name of the data source
     * @param sourceTopic - Name of the topic for this defined SourceName
     * @param xformMeta - Transform meta information
     * @param writer - IngestWriter which takes action for data received to this route
     */
    private void updateRouteMap(String sourceName, String sourceTopic, String xformMeta, TransformerWriter writer){
        //Inject new route
        if(!this.routeTable.containsKey(sourceName)){
            WriterHandler wh = new WriterHandler(xformMeta, writer);
            HashMap<String, WriterHandler> newRoute = new HashMap<>();
            newRoute.put(sourceTopic, new WriterHandler(xformMeta, writer));
            routeTable.put(sourceName, newRoute);
        }else{
            //Update an existing route if a diff is detected
            HashMap<String, WriterHandler> existingRoute = this.routeTable.get(sourceName);

            existingRoute.put(sourceTopic, new WriterHandler(xformMeta, writer));
        }
    }

    /**
     * Remove all routes which are not detailed in the metaEntries collection
     * @param metaEntries - metaEntries obtained from MetaRegistryLoader.fetchMetaEntries()
     */
    private void pruneRouteMap(Iterable<MetaEntry> metaEntries){
        //for each entry in routeTable, check to see if it exists in metaEntries
        //A match is sourceName + sourceTopic + xformMeta
        //Otherwise, remove from routeTable
        Iterator<Map.Entry<String, HashMap<String, WriterHandler>>> entrySourceIt = this.routeTable.entrySet().iterator();
        while (entrySourceIt.hasNext()) {
            Map.Entry<String, HashMap<String, WriterHandler>> entry = entrySourceIt.next();

            String existingSource = entry.getKey();
            HashMap<String, WriterHandler> topicMap = entry.getValue();

            Iterator<Map.Entry<String, WriterHandler>> entryTopicIt = topicMap.entrySet().iterator();
            while (entryTopicIt.hasNext()) {
                Map.Entry<String, WriterHandler> topicEntry = entryTopicIt.next();
                String existingTopic = topicEntry.getKey();
                WriterHandler existingWriterHandler = topicEntry.getValue();

                if(!sourceExistsInMetaEntries(metaEntries, existingSource)){
                    entrySourceIt.remove();
                }else{
                    if(!topicExistsInMetaEntries(metaEntries, existingSource, existingTopic)){
                        entryTopicIt.remove();

                        //If this source does not contain additional topics, remove source
                        if(this.routeTable.get(existingSource).size() == 0){
                            entrySourceIt.remove();
                        }
                    }

                }
            }
        }
    }

    /**
     * Check if a given topic matches a topic within the route table. Supports wildcard /* use
     * @param sourceTopic - The fully qualified topic received from a source
     * @param routeTopic - The topic registered within the route table
     * @param wildcard - True to turn on wildcard matching, otherwise only exact matches are accepted.
     * @return - True if a topic match was found
     */
    private boolean topicMatch(String sourceTopic, String routeTopic, boolean wildcard){
        if (wildcard){
            return sourceTopic.startsWith(routeTopic.substring(0, routeTopic.length() - 2));
        }else
            return routeTopic.equals(sourceTopic);
    }

    /**
     * Obtain a route's writer if the route identified by sourceProvider and sourceTopic exists
     * @param sourceProvider
     * @param sourceTopic
     * @return - The IngestWriter registered for a particular route. Null writer if no route was found.
     */
    public TransformerWriter getIngestWriter(String sourceProvider, String sourceTopic){
        if(routeTable.containsKey(sourceProvider)){
            HashMap<String, WriterHandler> topics = routeTable.get(sourceProvider);
            if (topics.containsKey(sourceTopic)){
                return topics.get(sourceTopic).writer; //short-circuit to avoid the scan
            }

            for(String routeTopic : topics.keySet()){
                if(routeTopic.endsWith("/*")){
                    if(topicMatch(sourceTopic, routeTopic, true)){
                        return routeTable.get(sourceProvider).get(routeTopic).writer;
                    }
                }
            }
        }
        return null;
    }

    /**
     * @return Cloned object with deep copy
     * @throws CloneNotSupportedException
     */
    public Object clone() throws CloneNotSupportedException {
        // Assign the shallow copy to new reference variable t
        RouteTable rtNew = (RouteTable) super.clone();

        //Copy existing route table values into this new object
        HashMap<String, HashMap<String, WriterHandler>> clonedRouteTableMap = new HashMap<>();
        for(String source : routeTable.keySet()){

            clonedRouteTableMap.put(source, new HashMap<>());

            for(String topic: routeTable.get(source).keySet()){
                String xformName = routeTable.get(source).get(topic).xformName;
                TransformerWriter writer = routeTable.get(source).get(topic).writer;

                clonedRouteTableMap.get(source).put(topic, new WriterHandler(xformName, writer));
            }
        }

        rtNew.routeTable = clonedRouteTableMap;

        return rtNew;
    }

    @Override
    public String toString() {
        StringBuilder retStr = new StringBuilder();

        for(String source : routeTable.keySet()){

            retStr.append("\nsource: ").append(source).append("\n");

            for(String topic: routeTable.get(source).keySet()){
                String xformName = routeTable.get(source).get(topic).xformName;
                TransformerWriter writer = routeTable.get(source).get(topic).writer;

                retStr.append("\ttopic: ").append(topic).append("\n");
                retStr.append("\t\txform: ").append(xformName).append("\n");
                retStr.append("\t\twriter: ").append(writer).append("\n");
            }
        }

        return retStr.toString();
    }


    /**
     * Internal class to make hashmap structure of route table more manageable
     */
    public class WriterHandler{
        public String xformName;
        public TransformerWriter writer;

        public WriterHandler(String xform, TransformerWriter writer){
            this.xformName = xform;
            this.writer = writer;
        }
    }

}
