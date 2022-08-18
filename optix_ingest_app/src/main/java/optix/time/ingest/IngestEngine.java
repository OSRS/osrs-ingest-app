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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * IngestEngine is the core Singleton class to host the entire lifecycle of the Generalized Ingest Application (GIA).
 * This can be used as a service by supporting the Initialize -&gt; Start -&gt; Stop -&gt; Start -&gt; Stop ... semantics.
 * This engine will initialize all sources, writers and routes (in that order) during initialize
 */
public class IngestEngine {
    final static Log logger = LogFactory.getLog(IngestEngine.class);
    private final Object syncRoot=new Object();

    private String deploymentName;
    private final IngestRouter router=new IngestRouter();
    private final HashMap<String, IngestSource> sources=new HashMap<>();
    private final HashMap<String, IngestWriter> writers=new HashMap<>();
    private final HashMap<String, IngestConfiguration.TypeConfiguration> types=new HashMap<>();

    private ExecutorService pool;
    private Future<?> mon;
    private RunState state= RunState.Created;
    private static IngestEngine instance;

    /**
     * get the name of this deployment
     * @return
     */
    public String getDeploymentName(){
        return this.deploymentName;
    }

    /**
     * get a reference to the configured Singleton IngestRouter
     * This does not change of the life of the application and is safe to retain a reference to.
     * @return The configured IngestRouter
     */
    public IngestRouter getRouter(){
        return this.router; //handles all data routing between sources and writers
    }

    /**
     * Gets a specific IngestSource by name as specified in the configuration
     * @param name
     * @return
     */
    public IngestSource getSource(String name){
        try {
            if (sources.containsKey(name))
                return sources.get(name);
        }catch (Exception e){ logger.error("", e); }
        return null;
    }

    /**
     * Gets a specific IngestWriter by name as specified in the configuration
     * @param name
     * @return
     */
    public IngestWriter getWriter(String name){
        try {
            if (writers.containsKey(name))
                return writers.get(name);
        }catch (Exception e){ logger.error("", e); }
        return null;
    }

    /**
     * Gets a specific TypeConfiguration by name as specified in the configuration
     * @param name
     * @return
     */
    public IngestConfiguration.TypeConfiguration getType(String name){
        try {
            if (types.containsKey(name))
                return types.get(name);
        }catch (Exception e){ logger.error("", e); }
        return null;
    }

    /**
     * @return the current RunState
     */
    public RunState getState(){
        return this.state;
    }

    /**
     * Initializes all sources and the IngestRouter
     * Note that we initialize the config first, then get all type configurations, then initialize sources, then initialize writers, then finally initialize the router
     * @return
     */
    public boolean initialize(){
        synchronized (this.syncRoot) {
            if (this.state == RunState.Created || this.state == RunState.Failed) {
                this.state = RunState.Transitioning;
                logger.info("Initializing optix.time.ingest.IngestEngine");

                //add reading config setting up items
                IngestConfiguration cfg = IngestConfiguration.getInstance();
                if (cfg.initialize()) {
                    this.deploymentName = cfg.getName();
                    logger.info("Registering Types");
                    for (IngestConfiguration.TypeConfiguration cur : IngestConfiguration.getInstance().getTypes()) {
                        if (cur != null && cur.getName().length() > 0 && cur.hasClass()) {
                            types.put(cur.getName(), cur);
                        }
                    }

                    for (IngestConfiguration.SourceConfiguration item : cfg.getSources()) {
                        IngestSource s = cfg.getType(item.getType(), IngestConfiguration.TypeCategory.DataSource).createIngestSource();
                        if (s != null && s.initialize(item.getName())) {
                            this.sources.put(item.getName(), s);
                        }
                    }

                    for (IngestConfiguration.WriterConfiguration item : cfg.getWriters()) {
                        IngestWriter s = cfg.getType(item.getType(), IngestConfiguration.TypeCategory.Writer).createIngestWriter();
                        if (s != null && s.initialize(item.getName())) {
                            this.writers.put(item.getName(), s);
                        }
                    }

                    if (this.router.initialize("")) {
                        logger.info("Initialized optix.time.ingest.IngestEngine");
                        this.state = RunState.Initialized;
                        return true;
                    }
                }

                logger.error("optix.time.ingest.IngestEngine initialization failed");
                this.state = RunState.Failed;
            }
            return false;
        }
    }

    /**
     * Starts up the executor service for all workers and the monitor for ensuring all items continue to stay in the running state.
     * Any Source or Writer that transitions to "Failed" will be restarted.
     * @return
     */
    public boolean start(){
        synchronized (this.syncRoot) {
            if (this.state == RunState.Initialized || this.state == RunState.Stopped) {
                this.state = RunState.Transitioning;
                logger.info("Starting optix.time.ingest.IngestEngine");

                boolean fails = false;
                for (IngestWriter w : this.writers.values()) {
                    try {
                        fails = fails || !w.start();
                    } catch (Exception e) {
                        logger.error("IngestEngine.start", e);
                        fails = true;
                    }
                }

                try {
                    fails = fails || !this.getRouter().start(); //this will ensure there's a route for records
                } catch (Exception e) {
                    logger.error("IngestEngine.start", e);
                    fails = true;
                }

                for (IngestSource s : this.sources.values()) {
                    try {
                        fails = fails || !s.start();
                    } catch (Exception e) {
                        logger.error("IngestEngine.start", e);
                        fails = true;
                    }
                }

                if (fails) {
                    logger.error("optix.time.ingest.IngestEngine start failed");
                    this.state = RunState.Failed;
                } else {
                    logger.info("optix.time.ingest.IngestEngine started");
                    this.pool = Executors.newSingleThreadExecutor();
                    this.state = RunState.Running;
                    this.mon = this.pool.submit(this::monitor);
                }
                return fails;
            }
            return false;
        }
    }

    /**
     * monitors all running sources, writers and the router for failure and restarted if needed.
     */
    private void monitor(){
        while(this.state == RunState.Running){
            if (this.state == RunState.Running) {
                for (IngestWriter w : this.writers.values()) {
                    try {
                        if (w.getState() == RunState.Failed)
                            w.start();
                    } catch (Exception e) {
                        logger.error("IngestEngine.monitor", e);
                    }
                }
            }

            if (this.state == RunState.Running) {
                try {
                    if (this.getRouter().getState() == RunState.Failed)
                        this.getRouter().start();
                } catch (Exception e) {
                    logger.error("IngestEngine.monitor", e);
                }
            }

            if (this.state == RunState.Running) {
                for (IngestSource s : this.sources.values()) {
                    try {
                        if (s.getState() == RunState.Failed)
                            s.start();
                    } catch (Exception e) {
                        logger.error("IngestEngine.monitor", e);
                    }
                }
            }
        }
    }
    private void shutdownAndAwaitTermination() {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    logger.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    /**
     * stops all workers in this order:
     *   stop the monitor
     *   stop each IngestSource
     *   stop the IngestRouter
     *   stop each IngestWriter
     * @return
     */
    public boolean stop(){
        synchronized (this.syncRoot) {
            if (this.state == RunState.Running || this.state == RunState.Failed) {
                this.state = RunState.Transitioning;
                logger.info("Stopping optix.time.ingest.IngestEngine");

                //stop the monitor first
                if (this.pool != null) {
                    if (this.mon != null) {
                        this.mon.cancel(true);
                    }
                    shutdownAndAwaitTermination();
                    this.mon = null;
                    this.pool = null;
                }

                //Stop readers then writers
                boolean fails = false;
                for (IngestSource s : this.sources.values()) {
                    try {
                        fails = fails || !s.stop();
                    } catch (Exception e) {
                        logger.error("IngestEngine.stop", e);
                        fails = true;
                    }
                }

                try {
                    fails = fails || !this.getRouter().stop(); //this will halt any new records from being routed
                } catch (Exception e) {
                    logger.error("IngestEngine.stop", e);
                    fails = true;
                }

                for (IngestWriter w : this.writers.values()) {
                    try {
                        fails = fails || !w.stop();
                    } catch (Exception e) {
                        logger.error("IngestEngine.stop", e);
                        fails = true;
                    }
                }

                if (fails) {
                    logger.error("optix.time.ingest.IngestEngine stop failed");
                    this.state = RunState.Failed;
                } else {
                    logger.info("optix.time.ingest.IngestEngine stopped");
                    this.state = RunState.Stopped;
                }

                return fails;
            }
            return false;
        }
    }

    /**
     * Gets the singular Singleton instance of the IngestEngine
     * @return
     */
    public static IngestEngine getInstance(){
        if (instance==null)
            instance=new IngestEngine();
        return instance;
    }

    private IngestEngine(){
        logger.info("Creating optix.time.ingest.IngestEngine");
    }
}
