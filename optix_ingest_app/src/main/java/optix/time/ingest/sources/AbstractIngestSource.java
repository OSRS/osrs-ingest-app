/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package optix.time.ingest.sources;

import optix.time.ingest.RunState;
import optix.time.ingest.writers.AbstractIngestWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This is the base abstract implementation of an IngestSource.
 * This implementation handles the basic state transitions for initialize, start, stop.
 * The initImpl method is the delegated initialization routine.
 * The run method is the delegated execution routine, which is invoked on a dedicated thread.
 * Implementors can choose whether to do all work on this thread or to spin up other threads.
 */
public abstract class AbstractIngestSource implements IngestSource{
    final static Log logger = LogFactory.getLog(AbstractIngestWriter.class);
    protected final Object syncRoot=new Object(); //just for sync in this object

    private String name;
    protected String getName(){return this.name;}

    @Override
    public final boolean initialize(String name) {
        synchronized (syncRoot) {
            if (this.state == RunState.Created || this.state == RunState.FailedInitialization) {
                this.state = RunState.Transitioning;
                logger.info("Initializing " + name);
                this.name=name;
                if (initImpl()) {
                    this.state = RunState.Initialized;
                    return true;
                }
            }
            this.state = RunState.FailedInitialization;
            return false;
        }
    }

    /**
     * This is the initialization routine to implement. Upon successful completion of this method, the object should be fully ready to be started and run.
     * @return A boolean indicating of initialization was successful. If false, this object will transition to the FailedInitialization state
     */
    protected abstract boolean initImpl();

    private Future<?> runner;
    private ExecutorService executor;
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

    @Override
    public final boolean start() {
        synchronized (syncRoot) {
            if (this.state == RunState.Stopped || this.state == RunState.Initialized || this.state == RunState.Failed) {
                this.state = RunState.Transitioning;

                logger.info("Starting " + this.name);
                //kill the prior executor if it's still there and we're restarting
                if (this.executor !=null){
                    if (this.runner!=null){
                        try{
                            //we're not trying to be nice since we're already supposed to be stopped
                            if (!(this.runner.isDone() || this.runner.isCancelled()))
                                this.runner.cancel(true);
                        }catch (Exception e){}
                    }

                    try {
                        if (!this.executor.isShutdown())
                            shutdownAndAwaitTermination();
                    }catch (Exception e){}
                }
                this.executor = Executors.newSingleThreadExecutor();
            }else
                return false;
        }

        //exit the sync block before returning
        this.state = RunState.Running;
        //start the writer on the runner thread so we don't block
        this.runner = this.executor.submit(this::run);

        return true;
    }

    /**
     * Developers implement this method to loop indefinitely, or to return immediately after initializing their own threading model
     * This method or the developers implementing work methods should check that this.getState()==RunState.Running and exit as soon as this is untrue
     */
    protected abstract void run();

    @Override
    public final boolean stop() {
        synchronized (syncRoot) {
            if (this.state == RunState.Running) {
                this.state = RunState.Transitioning;
                //do whatever to stop doing your async work
                logger.info("Attempting to stop " + this.name);

                if (this.runner!=null) {
                    boolean cleanShutdown=true;
                    try {
                        //wait to allow it to figure out it needs to shut down
                        int i=0;
                        while (!this.runner.isDone() && i<3) {
                            Thread.sleep(15000); //15 seconds
                            i++;
                        }
                    }catch (Exception e){}
                    try {
                        cleanShutdown = this.stopImpl();
                    }catch (Exception e){}
                    try {
                        if (!this.runner.isDone()) {
                            //cancel it
                            this.runner.cancel(false);
                        }
                    }catch (Exception e){}
                    try {
                        shutdownAndAwaitTermination(); //we don't have to be nice
                        this.runner = null;

                        if (cleanShutdown) {
                            this.state = RunState.Stopped;
                            logger.info(this.name + " stopped");
                            return true;
                        }
                    }catch (Exception e){}
                }
                logger.error(this.name + " failed stopping");
                this.state = RunState.Failed; //we didn't return early
            }
        }
        return false;
    }

    /**
     * This method is an overridable virtual method to allow the implementor to gracefully stop.
     * The public stop() method first transitions state and attempts to allow the run thread to stop.
     * Prior to cancelling and finalizing the thread this method will be called to allow for clean stopping and cleanup.
     * Upon return from this method, the run thread will be terminated if it is not complete.
     * @return A boolean indicating if stop is successful. If false, the runstate will transition to Failed rather than Stopped.
     */
    protected boolean stopImpl(){
        return true;
    }

    private RunState state=RunState.Created;
    @Override
    public RunState getState() {
        return this.state;
    }

    /**
     * Developers should invoke this method to indicate the writer has failed and should no longer do work.
     * This is effectively a self-initiated "stop" call
     * Any worker threads should be halted in the event of a failure, which is the responsibility of the implementer to do.
     */
    protected void setFailed(){
        this.state = RunState.Failed;
        logger.error("setFailed from "+this.name);
    }

    protected AbstractIngestSource(){}
}
