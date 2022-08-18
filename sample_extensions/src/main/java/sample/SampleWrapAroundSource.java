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

import optix.time.ingest.IngestEngine;
import optix.time.ingest.IngestRouter;
import optix.time.ingest.sources.AbstractIngestSource;

/**
 * This class, when paired with the SampleWrapAroundSourceWriter, will provide a "wrap-around" pipeline to allow messages to have a second path through the transform pipelines.
 * This allows this source to receive messages from a SampleWrapAroundSourceWriter and to then re-write them back into the pipeline, generally as a means of routing messages to a different topic.
 */
public final class SampleWrapAroundSource extends AbstractIngestSource {
    private IngestRouter router;

    /**
     * This method provides a basic implementation for initializing a source.
     * All sources will write through the IngestRouter configured within the IngestEngine and therefore taking an early reference that can be persisted is a means to avoid re-acquiring the reference on each call.
     * @return
     */
    @Override
    protected boolean initImpl() {
        this.router= IngestEngine.getInstance().getRouter();
        return true;
    }

    @Override
    protected void run() {
        return; //we don't need to do anything
    }

    public boolean write(final String sourceTopic, final String record){
        return router.write(this.getName(), sourceTopic, record);
    }

    public boolean write(final String sourceTopic, final Iterable<String> records){
        return router.write(this.getName(), sourceTopic, records);
    }

    public boolean writeBinary(final String sourceTopic, final byte[] record){
        return router.writeBinary(this.getName(), sourceTopic, record);
    }

    public boolean writeBinary(final String sourceTopic, final Iterable<byte[]> records){
        return router.writeBinary(this.getName(), sourceTopic, records);
    }
}
