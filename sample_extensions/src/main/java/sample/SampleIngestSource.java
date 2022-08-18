/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package sample;

import optix.time.ingest.IngestConfiguration;
import optix.time.ingest.IngestEngine;
import optix.time.ingest.IngestRouter;
import optix.time.ingest.RunState;

import optix.time.ingest.sources.AbstractIngestSource;
import optix.time.ingest.sources.IngestSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;

/**
 * This class is an example IngestSource that implements the IngestSource interface via the AbstractIngestSource abstract implementation.
 * This class simply writes and infinite set of basic textual content to whichever routes are available to it.
 * This class can be used / modified as needed to provide a testing data source that writes simple data to the configured flow(s).
 */
public final class SampleIngestSource extends AbstractIngestSource {
    private IngestRouter router;

    @Override
    public boolean initImpl() {
        IngestConfiguration.SourceConfiguration cfg = IngestConfiguration.getInstance().getSource(this.getName());
        if (cfg != null) {
            this.router=IngestEngine.getInstance().getRouter();
            return true;
        }
        return false;
    }

    @Override
    public void run() {
        //This ingest source emits a message on an interval
        while(this.getState() == RunState.Running){
            try{
                Thread.sleep(100);
                //No-op writer
                write("topic/data7", "Don't transform me!");
//                this.router.writeBinary(this.getName(), "topic/data5", "hello".getBytes(StandardCharsets.UTF_8));

                //transform writer
                write("topic/data7", "Hello!");

                //Sample of generating an iterable of records for processing with a transformation
                ArrayList<byte[]> records = new ArrayList<>();
                records.add("{\"metric\": \"AirTemp\", \"value\": 30, \"address\": \"00:a0:49:05:19:15\", \"timestamp\": \"2019-10-31 18:00:00.123\"}".getBytes(StandardCharsets.UTF_8));
                records.add("{\"metric\": \"WattUsage\", \"value\": 99, \"address\": \"00:a0:49:05:19:15\", \"timestamp\": \"2019-10-31 18:00:00.123\"}".getBytes(StandardCharsets.UTF_8));
                this.router.writeBinary(this.getName(), "topic/data5", records);

                ArrayList<String> strRecords = new ArrayList<>();
                strRecords.add("msg1");
                strRecords.add("msg2");

                write("topic/data7", strRecords);

                write("foobar", "it's a message");

            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    //Since we are listening to all topics, the topic we send to the router is the specific message we received from the broker

    private void write(String topic, String msg){
        this.router.write(this.getName(), topic, msg);
//        this.router.writeBinary(this.name, topic, msg.getBytes(StandardCharsets.UTF_8));
    }

    private void write(String topic, Iterable<String> records){
        this.router.write(this.getName(), topic, records);
//        this.router.writeBinary(this.name, topic, msg.getBytes(StandardCharsets.UTF_8));
    }

}


