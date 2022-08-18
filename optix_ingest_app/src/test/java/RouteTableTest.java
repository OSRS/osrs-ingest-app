/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
import optix.time.ingest.*;
import optix.time.ingest.RouteTable.WriterHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

public class RouteTableTest {

    RouteTable routeTable = null;

    @Before
    public void setUp() {
        // one-time initialization code
        // Use testing config
        this.routeTable = new RouteTable();
        System.setProperty("configFile", "target/test-classes/config.json");

        //Initialize the IngestEngine to load the test config file
        IngestEngine engine=IngestEngine.getInstance();
        if (engine!=null){
            try{
                engine.initialize();
            }catch (com.amazonaws.SdkClientException e){
                //Within tests, fetching Meta Entries from AWS lambda is likely to fail
                //This is ok, since we will be injecting custom Meta Entry information
            }
        }
    }

    //Add new entries to an empty route table
    @Test
    public void addNewEntriesToEmptyRouteTable() {
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Inspect the route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransformer", routeTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt", "newTopic"));

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt2").containsKey("newTopic"));
            Assert.assertEquals("",routeTableMap.get("input_mqtt2").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt2", "newTopic"));

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }



    //Update existing entries within a route table
    @Test
    public void updateXformEntriesWithinRouteTable() {

        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Simulate Update
            ArrayList<MetaEntry> refreshMetaEntries = new ArrayList<>();
            refreshMetaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransform2Altered"));
            refreshMetaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(refreshMetaEntries);

            //Inspect the route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransform2Altered", routeTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt", "newTopic"));

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }

    //Add a new topic to an existing source
    @Test
    public void addNewTopicEntriesWithinRouteTable() {
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Simulate Update
            ArrayList<MetaEntry> refreshMetaEntries = new ArrayList<>();
            refreshMetaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            refreshMetaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));
            refreshMetaEntries.add(new MetaEntry("input_mqtt2", "newTopicInSource", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(refreshMetaEntries);

            //Inspect the route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(2, routeTableMap.get("input_mqtt2").size());
            Assert.assertEquals(true, routeTableMap.get("input_mqtt2").containsKey("newTopicInSource"));

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }

    //Remove entries from a route table which do not occur in an updated meta entries list

    //Remove sources which no longer exist in Meta Entries
    @Test
    public void pruneSourceFromRouteTable(){
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Simulate Update
            ArrayList<MetaEntry> refreshMetaEntries = new ArrayList<>();
            refreshMetaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));

            routeTable.updateRoutes(refreshMetaEntries);

            //Inspect the route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            Assert.assertEquals(1, routeTableMap.size());
            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt"));

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }


    //Remove topics which no longer exist within sources
    @Test
    public void pruneTopicFromSource(){
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic2", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Simulate Update
            ArrayList<MetaEntry> refreshMetaEntries = new ArrayList<>();
            refreshMetaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));

            routeTable.updateRoutes(refreshMetaEntries);

            //Inspect the route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            //After pruning, only 1 topic should remain for this source
            Assert.assertEquals(1, routeTableMap.get("input_mqtt").size());
            Assert.assertEquals(false, routeTableMap.get("input_mqtt").containsKey("newTopic2"));

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }

    @Test
    public void sourceExistsInMetaEntries(){
        try{
            Method method = routeTable.getClass().getDeclaredMethod("sourceExistsInMetaEntries", Iterable.class,String.class);
            method.setAccessible(true);

            //Build the new Meta Entry List
            //This simulates receiving new config information
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("newSource","newTopic","","",100, "lambda:newTransform"));
            metaEntries.add(new MetaEntry("newSource2","newTopic2","","",100, "lambda:newTransform2"));

            Assert.assertEquals(true, method.invoke(routeTable, metaEntries, "newSource"));
            Assert.assertEquals(false, method.invoke(routeTable, metaEntries, "sourceThatDoesNotExist"));

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void topicExistsInMetaEntries(){
        try{
            Method method = routeTable.getClass().getDeclaredMethod("topicExistsInMetaEntries", Iterable.class, String.class, String.class);
            method.setAccessible(true);

            //Build the new Meta Entry List
            //This simulates receiving new config information
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("newSource","newTopic","","",100, "lambda:newTransform"));
            metaEntries.add(new MetaEntry("newSource2","newTopic2","","",100, "lambda:newTransform2"));

            Assert.assertEquals(true, method.invoke(routeTable, metaEntries, "newSource", "newTopic"));
            Assert.assertEquals(false, method.invoke(routeTable, metaEntries, "newSource", "topicThatDoesNotExist"));

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void topicExactMatch(){
        try{
            Method method = routeTable.getClass().getDeclaredMethod("topicMatch", String.class, String.class, boolean.class);
            method.setAccessible(true);

            Assert.assertEquals(true, method.invoke(routeTable, "topic/3", "topic/3", false));
            Assert.assertEquals(false, method.invoke(routeTable, "topic/2", "topic/3", false));

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void topicWildcardMatch(){
        try{
            Method method = routeTable.getClass().getDeclaredMethod("topicMatch", String.class, String.class, boolean.class);
            method.setAccessible(true);

            Assert.assertEquals(true, method.invoke(routeTable, "topic/3", "topic/3/*", true));
            Assert.assertEquals(true, method.invoke(routeTable, "topic/3/more", "topic/3/*", true));
            Assert.assertEquals(false, method.invoke(routeTable, "topic/2/more", "topic/3/*", true));
            Assert.assertEquals(false, method.invoke(routeTable, "topic/2", "topic/3/*", true));

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //Performing mutations on a cloned route table should be safe to the original object. These check that insert, update,
    // and deletes do not affect the original object and that a deep copy was performed.
    @Test
    public void cloneInsertTest(){
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Clone the route table
            RouteTable clonedRouteTable = (RouteTable) routeTable.clone();

            //Simulate updating the cloned table
            ArrayList<MetaEntry> refreshedMetaEntries = new ArrayList<>();
            refreshedMetaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            refreshedMetaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));
            refreshedMetaEntries.add(new MetaEntry("input_mqtt2", "newTopic2", "output_mqtt", "outputTopic", 100, ""));

            clonedRouteTable.updateRoutes(refreshedMetaEntries);

            //Inspect the existing route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            //Verify integrity of existing route table
            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransformer", routeTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt", "newTopic"));

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt2").containsKey("newTopic"));
            Assert.assertEquals("",routeTableMap.get("input_mqtt2").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt2", "newTopic"));

            Assert.assertEquals(1, routeTableMap.get("input_mqtt2").size());


            //Inspect the cloned route table
            Object clonedRouteTableObject = field.get(clonedRouteTable);
            HashMap<String, HashMap<String, WriterHandler>> clonedRouteTableMap = (HashMap<String, HashMap<String, WriterHandler>>) clonedRouteTableObject;

            //Verify alterations to cloned route table
            Assert.assertEquals(true, clonedRouteTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, clonedRouteTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransformer", clonedRouteTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(clonedRouteTable.getIngestWriter("input_mqtt", "newTopic"));

            Assert.assertEquals(true, clonedRouteTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, clonedRouteTableMap.get("input_mqtt2").containsKey("newTopic"));
            Assert.assertEquals("",clonedRouteTableMap.get("input_mqtt2").get("newTopic").xformName);
            Assert.assertNotNull(clonedRouteTable.getIngestWriter("input_mqtt2", "newTopic"));

            Assert.assertEquals(true, clonedRouteTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, clonedRouteTableMap.get("input_mqtt2").containsKey("newTopic2"));
            Assert.assertEquals("",clonedRouteTableMap.get("input_mqtt2").get("newTopic2").xformName);
            Assert.assertNotNull(clonedRouteTable.getIngestWriter("input_mqtt2", "newTopic2"));

            Assert.assertEquals(2, clonedRouteTableMap.get("input_mqtt2").size());

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }

    @Test
    public void cloneUpdateTest(){
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Clone the route table
            RouteTable clonedRouteTable = (RouteTable) routeTable.clone();

            //Simulate updating the cloned table
            ArrayList<MetaEntry> refreshedMetaEntries = new ArrayList<>();
            refreshedMetaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            refreshedMetaEntries.add(new MetaEntry("input_mqtt2", "newTopic2", "output_mqtt", "outputTopic", 100, ""));

            clonedRouteTable.updateRoutes(refreshedMetaEntries);

            //Inspect the existing route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            //Verify integrity of existing route table
            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransformer", routeTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt", "newTopic"));

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt2").containsKey("newTopic"));
            Assert.assertEquals("",routeTableMap.get("input_mqtt2").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt2", "newTopic"));


            //Inspect the cloned route table
            Object clonedRouteTableObject = field.get(clonedRouteTable);
            HashMap<String, HashMap<String, WriterHandler>> clonedRouteTableMap = (HashMap<String, HashMap<String, WriterHandler>>) clonedRouteTableObject;

            //Verify alterations to cloned route table
            Assert.assertEquals(true, clonedRouteTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, clonedRouteTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransformer", clonedRouteTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(clonedRouteTable.getIngestWriter("input_mqtt", "newTopic"));

            Assert.assertEquals(false, clonedRouteTableMap.get("input_mqtt2").containsKey("newTopic"));

            Assert.assertEquals(true, clonedRouteTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, clonedRouteTableMap.get("input_mqtt2").containsKey("newTopic2"));
            Assert.assertEquals("",clonedRouteTableMap.get("input_mqtt2").get("newTopic2").xformName);
            Assert.assertNotNull(clonedRouteTable.getIngestWriter("input_mqtt2", "newTopic2"));

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }

    @Test
    public void cloneDeleteTest(){
        try{
            //Mock metaEntries
            //These map to actual entries in the test config
            ArrayList<MetaEntry> metaEntries = new ArrayList<>();
            metaEntries.add(new MetaEntry("input_mqtt", "newTopic", "output_mqtt", "outputTopic", 100, "lambda:newTransformer"));
            metaEntries.add(new MetaEntry("input_mqtt2", "newTopic", "output_mqtt", "outputTopic", 100, ""));

            routeTable.updateRoutes(metaEntries);

            //Clone the route table
            RouteTable clonedRouteTable = (RouteTable) routeTable.clone();

            //Simulate updating the cloned table
            ArrayList<MetaEntry> refreshedMetaEntries = new ArrayList<>();
            clonedRouteTable.updateRoutes(refreshedMetaEntries);

            //Inspect the existing route table
            final Field field = routeTable.getClass().getDeclaredField("routeTable");
            field.setAccessible(true);
            Object routeTableObject = field.get(routeTable);
            HashMap<String, HashMap<String, WriterHandler>> routeTableMap = (HashMap<String, HashMap<String, WriterHandler>>) routeTableObject;

            //Verify integrity of existing route table
            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt").containsKey("newTopic"));
            Assert.assertEquals("newTransformer", routeTableMap.get("input_mqtt").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt", "newTopic"));

            Assert.assertEquals(true, routeTableMap.containsKey("input_mqtt2"));
            Assert.assertEquals(true, routeTableMap.get("input_mqtt2").containsKey("newTopic"));
            Assert.assertEquals("",routeTableMap.get("input_mqtt2").get("newTopic").xformName);
            Assert.assertNotNull(routeTable.getIngestWriter("input_mqtt2", "newTopic"));


            //Inspect the cloned route table
            Object clonedRouteTableObject = field.get(clonedRouteTable);
            HashMap<String, HashMap<String, WriterHandler>> clonedRouteTableMap = (HashMap<String, HashMap<String, WriterHandler>>) clonedRouteTableObject;

            //Verify alterations to cloned route table
            Assert.assertEquals(0, clonedRouteTableMap.size());

        }catch(Exception e){
            e.printStackTrace();
            Assert.fail("Exception Detected");
        }
    }
}
