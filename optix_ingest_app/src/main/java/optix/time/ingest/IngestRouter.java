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

import optix.time.ingest.writers.IngestWriter;
import optix.time.ingest.writers.TransformerWriter;
import optix.time.ingest.writers.WorkPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * The IngestRouter class implements all core routing logic for messages and acts as a singular IngestWriter for all sources to write to.
 * All messages that are written to the IngestRouter either match a route and are passed forward, or match no route and are immediately dropped.
 */
public final class IngestRouter implements IngestWriter {
    final static Log logger = LogFactory.getLog(IngestRouter.class);
    private final Object syncRoot=new Object(); //just for sync in this object

    private MetaRegistry registry;
    private RouteTable routeTable = new RouteTable();

    private int targetThreads;
    private ExecutorService executor;
    private Future<?> updater;
    private final ArrayList<Future<?>> workers=new ArrayList<>();
    private final WorkPool messages=new WorkPool();

    private boolean running=false;

    /**
     * Initializes the IngestRouter and creates all initial routes from the route table.
     * This will fail if any named sources, writers or transforms cannot be created.
     * This will fail if the route table is unavailable.
     * @param name
     * @return
     */
    @Override
    public boolean initialize(String name) {
        synchronized (syncRoot) {
            if (this.state == RunState.Created || this.state == RunState.FailedInitialization) {
                this.state = RunState.Transitioning;
                if (registry==null){
                    logger.info("Initializing optix.time.ingest.IngestRouter");

                    //create sources - these do not change while running
                    logger.info("Creating Sources");

                    this.targetThreads = IngestConfiguration.getInstance().getTargetThreads();

                    //initialize routes - these do change while running
                    registry = new MetaRegistryLoader();
                    this.lastUpdate=Instant.EPOCH;

                    updateRoutes();

                    logger.info("optix.time.ingest.IngestRouter initialized");
                    this.state=RunState.Initialized;
                    return true;
                }
                this.state = RunState.FailedInitialization;
                logger.error("optix.time.ingest.IngestRouter initialization failed");
            }
            return this.state!=RunState.FailedInitialization; //so it returns true if we've already been initialized before
        }
    }

    /**
     * Called on a dedicated Executor thread, which runs on a never-ending loop to update the route table periodically.
     * This is currently set to 1 hour.
     * Todo - A future update is to load the update interval from configuration.
     */
    private void routeUpdater(){
        while(this.running) {
            Instant cur = Instant.now();
            if (cur.getEpochSecond() - this.lastUpdate.getEpochSecond() > 3600)
                updateRoutes();

            try {
                Thread.sleep(10000); //10 seconds
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private Instant lastUpdate;

    /**
     * Updates routes from the route table.
     * This will deal with all three change types: new routes, removed routes, modified routes.
     * Any routes that are unchanged in the table will be unmodified here.
     * Changed routes will be abandoned and will get garbage collected once they have no work to do.
     */
    public void updateRoutes(){
        logger.info("Updating Routes");

        //Do a deep copy of the current route table for updating / pruning
        //Re-assign to the global route table after.
        //This ensures thread safety when updates to route table occur
        RouteTable routeTableClone = null;
        try {
            routeTableClone = (RouteTable) routeTable.clone();
        } catch (CloneNotSupportedException e) {
            logger.error("", e);
        }

        if(routeTableClone != null){
            Iterable<MetaEntry> metaEntries = registry.fetchMetaEntries();
            routeTableClone.updateRoutes(metaEntries);
            this.routeTable = routeTableClone;
        }else{
            logger.error("Route Table Update Failed. Old version retained.");
        }

        this.lastUpdate=Instant.now();

        logger.info("Done Updating Routes");

        //Debug route table print helper
        logger.debug(this.routeTable.toString());
    }


    /**
     * writes the message into the internal message queue to be handled by the worker pool.
     * @param sourceProvider
     * @param sourceTopic
     * @param record
     * @return
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, String record) {
        return this.messages.write(sourceProvider, sourceTopic, record);
    }

    /**
     * writes the message into the internal message queue to be handled by the worker pool.
     * @param sourceProvider
     * @param sourceTopic
     * @param records
     * @return
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, Iterable<String> records) {
        return this.messages.write(sourceProvider, sourceTopic, records);
    }

    /**
     * writes the message into the internal message queue to be handled by the worker pool.
     * @param sourceProvider
     * @param sourceTopic
     * @param record
     * @return
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, byte[] record) {
        return this.messages.writeBinary(sourceProvider, sourceTopic, record);
    }

    /**
     * writes the message into the internal message queue to be handled by the worker pool.
     * @param sourceProvider
     * @param sourceTopic
     * @param records
     * @return
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        return this.messages.writeBinary(sourceProvider, sourceTopic, records);
    }

    /**
     * starts the Executor service and worker pool that handles all messaging and performs the initial dispatching of those worker threads.
     * additionally dispatches the update worker in the same pool to update the route table regularly.
     * @return
     */
    @Override
    public boolean start() {
        synchronized (syncRoot) {
            if (this.state == RunState.Stopped || this.state == RunState.Initialized || this.state == RunState.Failed) {
                this.state = RunState.Transitioning;

                logger.info("Starting IngestRouter");
                //kill the prior executor if it's still there and we're restarting
                if (this.executor != null) {
                    if (this.updater != null) {
                        try {
                            //we're not trying to be nice since we're already supposed to be stopped
                            if (!(this.updater.isDone() || this.updater.isCancelled()))
                                this.updater.cancel(true);
                        } catch (Exception e) {
                            logger.error("failed shutting down prior updater", e);
                        }
                    }

                    try {
                        if (!this.executor.isShutdown())
                            shutdownAndAwaitTermination();
                    } catch (Exception e) {
                        logger.error("failed shutting down prior executor service", e);
                    }
                }

                this.running=true;
                this.executor= Executors.newFixedThreadPool(this.targetThreads+1);
                this.updater = this.executor.submit(this::routeUpdater);
                this.workers.clear();
                for(int i=0;i<this.targetThreads;i++){
                    this.workers.add(this.executor.submit(this::workScavenge));
                }

                this.state = RunState.Running;
                return true;
            }
        }
        return false;
    }

    /**
     * stops the current execution of the router. This will stop all workers in the pool which will allow current messages to drain from the queues prior to exiting.
     * NOTE: calling stop does not fully guarantee all messages are written to the writers, instead it guarantees no new messages will be allowed into the queues during the stopping.
     * @return
     */
    @Override
    public boolean stop() {
        synchronized (this.syncRoot){
            if (this.state == RunState.Running){
                this.state = RunState.Transitioning;
                this.running=false;
                logger.info("Stopping IngestRouter");
                //kill the prior executor if it's still there and we're restarting
                if (this.executor != null) {
                    if (this.updater != null) {
                        try {
                            //we're not trying to be nice since we're already supposed to be stopped
                            if (!(this.updater.isDone() || this.updater.isCancelled()))
                                this.updater.cancel(true);
                        } catch (Exception e) {
                            logger.error("failed shutting down updater", e);
                        }
                    }

                    try{
                        Thread.sleep(2000); //give some time for workers to notice before we start killing
                    }catch (Exception ignored){}

                    boolean anyWorkers=true;
                    int waitCount=0; //we need to limit how long we wait
                    while(anyWorkers && waitCount<4) {
                        waitCount++;
                        anyWorkers=false;
                        for (Future<?> f : this.workers) {
                            //pause on any
                            if (!f.isDone()) {
                                anyWorkers=true;
                                try {
                                    Thread.sleep(2000); //give some time for workers to notice before we start killing
                                } catch (Exception ignored) {}
                            }
                        }
                    }

                    //be a bit more aggressive in asking to stop
                    for (Future<?> f : this.workers) {
                        if (!f.isDone())
                            f.cancel(false);
                    }

                    try {
                        //kill'em all yo
                        if (!this.executor.isShutdown())
                            shutdownAndAwaitTermination();
                    } catch (Exception e) {
                        logger.error("failed shutting down executor service", e);
                    }
                }
                this.state = RunState.Stopped;
            }
        }
        return true;
    }

    /**
     * this is the method the worker threads run to steal work from the queues and push them through the routes
     */
    private void workScavenge(){
        while(this.state == RunState.Running){
            MessageTuple<String> a = this.messages.stringMessages.poll();
            if (a!=null){
                IngestWriter routeWriter = routeTable.getIngestWriter(a.source, a.topic);
                if(routeWriter != null){
                    routeWriter.write(a.source, a.topic, a.record);
                }
            }

            MessageTuple<byte[]> b = this.messages.binaryMessages.poll();
            if (b!=null){
                TransformerWriter routeWriter = routeTable.getIngestWriter(b.source, b.topic);
                if(routeWriter != null){
                    routeWriter.writeBinary(b.source, b.topic, b.record);
                }
            }

            MessageTuple<Iterable<String>> c = this.messages.stringBagMessages.poll();
            if (c!=null){
                IngestWriter routeWriter = routeTable.getIngestWriter(c.source, c.topic);
                if(routeWriter != null){
                    routeWriter.write(c.source, c.topic, c.record);
                }
            }

            MessageTuple<Iterable<byte[]>> d = this.messages.binaryBagMessages.poll();
            if (d!=null){
                IngestWriter routeWriter = routeTable.getIngestWriter(d.source, d.topic);
                if(routeWriter != null){
                    routeWriter.writeBinary(d.source, d.topic, d.record);
                }
            }
        }
    }

    /**
     * this method stops the ExecutorService nicely if possible and harshly if not.
     */
    private void shutdownAndAwaitTermination() {
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                    logger.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private RunState state=RunState.Created;

    /**
     * Gets the current run state of the router.
     * @return
     */
    @Override
    public RunState getState() {
        return this.state;
    }
}
