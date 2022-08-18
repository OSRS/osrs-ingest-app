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

/**
 * this is a simple Singleton wrapper for the startup and stopping of the IngestEngine.
 * This class is suitable for use in one-time startup applications with a single clean shutdown.
 * This class is not suitable for use in daemon/service style applications that prefer to allow start/stop/start cycling. In this case, use the IngestEngine directly.
 */
public final class IngestHost implements Runnable{

    final static Log logger = LogFactory.getLog(IngestHost.class);

    /**
     * Initialize and start the IngestEngine
     */
    @Override
    public void run() {
        logger.info("Starting Host");
        IngestEngine engine=IngestEngine.getInstance();
        if (engine!=null){
            engine.initialize();
            if (engine.getState() == RunState.Initialized){
                engine.start();
            }
        }
        logger.info("Done Starting Host");
    }

    /**
     * Stop the IngestEngine cleanly
     */
    public void stop(){
        logger.info("Stopping Host");
        IngestEngine.getInstance().stop();
        logger.info("Done Stopping Host");
    }

    private static IngestHost instance=new IngestHost();
    public static IngestHost getInstance(){
        return instance;
    }

    private IngestHost(){
        logger.info("Constructing optix.time.ingest.IngestHost");
    }
}
