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
import optix.time.ingest.sources.IngestSource;
import optix.time.ingest.writers.AbstractSimpleIngestWriter;

/**
 * This class, when paired with a SampleWrapAroundSource, will provide a pathway to wrap messages through the pipeline another time, generally to a different source name / topic name.
 * Notice this class will find the configured outputSource this writer is associated with and writes all messages to that source.
 * This provides a mechanism that permits the fusion of multiple sources into a single source by routing multiple of these objects to a single SampleWrapAroundSource instance.
 */
public final class SampleWrapAroundSourceWriter extends AbstractSimpleIngestWriter {
    private String source;
    private SampleWrapAroundSource destination;

    @Override
    protected boolean initImpl() {
        IngestConfiguration.WriterConfiguration config = IngestConfiguration.getInstance().getWriter(this.getName());
        if (config!=null){
            if (config.containsKey("outputSource")){
                this.source=config.get("outputSource");

                IngestSource src = IngestEngine.getInstance().getSource(this.source);
                if (src!=null && src instanceof SampleWrapAroundSource){
                    this.destination = (SampleWrapAroundSource)src;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected void writeImpl(String sourceProvider, String sourceTopic, String record) {
        this.destination.write(sourceTopic, record);
    }

    @Override
    protected void writeImpl(String sourceProvider, String sourceTopic, Iterable<String> records) {
        this.destination.write(sourceTopic, records);
    }

    @Override
    protected void writeBinaryImpl(String sourceProvider, String sourceTopic, byte[] record) {
        this.destination.writeBinary(sourceTopic, record);
    }

    @Override
    protected void writeBinaryImpl(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        this.destination.writeBinary(sourceTopic, records);
    }
}
