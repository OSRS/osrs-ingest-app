/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
import optix.time.ingest.IngestHost;

import java.util.Scanner;

import optix.time.ingest.RunState;
import org.apache.commons.logging.*;

//Move this class to another package / jar separate from rest

/**
 * This class is a simple entry point application to start a stand-alone GeneralizedIngestApplication (GIA).
 * This class can be replaced with any other class that obeys the semantics of starting the IngestHost or IngestEngine.
 * Note this class main thread must not exit until after the IngestHost / IngestEngine properly stop or the entire process will simply stop.
 * This main thread is the only foreground blocking thread to keep this application running.
 *
 * Note here that this application simply waits for an enter key press from the application to trigger a shutdown.
 */
public class IngestApp {

    final static Log logger = LogFactory.getLog(IngestApp.class);

    public static void main(String[] args){
        logger.info("Application run location : " + System.getProperty("user.dir"));
        logger.info("Starting app now");
        IngestHost host = IngestHost.getInstance();
        host.run();

        //Keep main thread alive
        Scanner input = new Scanner(System.in);
        input.nextLine();

        IngestHost.getInstance().stop();

        logger.info("Exiting app now");

    }
}
