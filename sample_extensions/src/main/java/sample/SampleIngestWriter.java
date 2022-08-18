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

import optix.time.ingest.writers.AbstractSimpleIngestWriter;

import java.util.Base64;

/**
 * This class is a basic implementation of the IngestWriter interface via the AbstractSimpleIngestWriter abstract class.
 * Note this is the simplest way to implement an IngestWriter as all threading / initialization is handled.
 */
public final class SampleIngestWriter extends AbstractSimpleIngestWriter {
    private static final Base64.Encoder encoder = Base64.getEncoder();

    /**
     * Provides a basic initialization implementation. For this class, it does nothing but return true.
     * For other classes, this would implment any logic needed to ready the writer for working.
     * Note that there is also an available "stopImpl" method to permit stopping ingest.
     *
     * This method should not return until it is in a state ready for "run" to be called to start actually receiving data.
     * @return
     */
    @Override
    protected boolean initImpl() {
        return true;
    }

    @Override
    protected void writeImpl(String sourceProvider, String sourceTopic, String record) {
        System.out.print(sourceProvider);
        System.out.print(": ");
        System.out.print(sourceTopic);
        System.out.print(": ");
        System.out.println(record);
    }

    @Override
    protected void writeImpl(String sourceProvider, String sourceTopic, Iterable<String> records) {
        System.out.print(sourceProvider);
        System.out.print(": ");
        System.out.print(sourceTopic);
        System.out.print(": [");
        for(String item:records){
            System.out.print(item);
            System.out.print(", ");
        }
        System.out.println("]");
    }

    @Override
    protected void writeBinaryImpl(String sourceProvider, String sourceTopic, byte[] record) {
        System.out.print(sourceProvider);
        System.out.print(": ");
        System.out.print(sourceTopic);
        System.out.print(": ");
        System.out.println(encoder.encodeToString(record));
    }

    @Override
    protected void writeBinaryImpl(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        System.out.print(sourceProvider);
        System.out.print(": ");
        System.out.print(sourceTopic);
        System.out.print(": [");
        for(byte[] item:records){
            System.out.print(encoder.encodeToString(item));
            System.out.print(", ");
        }
        System.out.println("]");
    }
}
