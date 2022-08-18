/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package optix.time.ingest.writers;

import optix.time.ingest.MessageTuple;
import optix.time.ingest.RunState;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is an abstract implementation of a simple IngestWriter that allows the write methods to be async.
 * The write methods all enqueue the records while having the run method spin through all queued records on a dedicated thread.
 * NOTE: there is no throttling to drop messages if the queues back up and there is only a single thread performing ingest.
 */
public abstract class AbstractSimpleIngestWriter extends AbstractIngestWriter {
    private final ConcurrentLinkedQueue<MessageTuple<String>> stringMessages=new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<MessageTuple<byte[]>> binaryMessages=new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<MessageTuple<Iterable<String>>> stringBagMessages=new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<MessageTuple<Iterable<byte[]>>> binaryBagMessages=new ConcurrentLinkedQueue<>();

    /**
     * This is the actual execution loop that runs on the dedicated ExecutorService thread to dequeue all records for processing.
     * Any class that extends this class will benefit from a built-in model of threading provided by the IngestEngine startup for IngestWriters and IngestSources.
     */
    @Override
    protected void run() {
        while(this.getState() == RunState.Running){
            MessageTuple<String> a = this.stringMessages.poll();
            if (a!=null)
                this.writeImpl(a.source, a.topic, a.record);

            MessageTuple<byte[]> b = this.binaryMessages.poll();
            if (b!=null)
                this.writeBinaryImpl(b.source, b.topic, b.record);

            MessageTuple<Iterable<String>> c = this.stringBagMessages.poll();
            if (c!=null)
                this.writeImpl(c.source, c.topic, c.record);

            MessageTuple<Iterable<byte[]>> d = this.binaryBagMessages.poll();
            if (d!=null)
                this.writeBinaryImpl(d.source, d.topic, d.record);
        }
    }

    /**
     * This implementation of write extends the standard model to enqueue the record into a record queue for processing.
     * This call is syncrhonous in the caller, whereas handling of the message ultimately is done by a dedicated Executor thread via the run loop.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param record The actual record body. This will be a string which is encoded in a manner specific to the pipeline. the record may be null or empty (0 length).
     * @return
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, String record) {
        if (record!=null) {
            this.stringMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, record));
            return true;
        }
        return false;
    }

    /**
     * Implementors implement this method to be called syncrhonously by the run loop Executor thread upon successful dequeue of a message.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param record The actual record body. This will be a string which is encoded in a manner specific to the pipeline. the record may be null or empty (0 length).
     */
    protected abstract void writeImpl(String sourceProvider, String sourceTopic, String record);

    /**
     * This implementation of write extends the standard model to enqueue the record into a record queue for processing.
     * This call is syncrhonous in the caller, whereas handling of the message ultimately is done by a dedicated Executor thread via the run loop.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param records An Iterable&lt;String&gt; of record strings, behavior should be similar to sending each record individually.
     * @return
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, Iterable<String> records) {
        if (records!=null) {
            this.stringBagMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, records));
            return true;
        }
        return false;
    }

    /**
     * Implementors implement this method to be called syncrhonously by the run loop Executor thread upon successful dequeue of a message.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param records An Iterable&lt;String&gt; of record strings, behavior should be similar to sending each record individually.
     */
    protected abstract void writeImpl(String sourceProvider, String sourceTopic, Iterable<String> records);

    /**
     * This implementation of write extends the standard model to enqueue the record into a record queue for processing.
     * This call is syncrhonous in the caller, whereas handling of the message ultimately is done by a dedicated Executor thread via the run loop.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param record The actual record body. This will be a byte array which is encoded in a manner specific to the pipeline. the record may be null or empty (0 length).
     * @return
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, byte[] record) {
        if (record!=null) {
            this.binaryMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, record));
            return true;
        }
        return false;
    }

    /**
     * Implementors implement this method to be called syncrhonously by the run loop Executor thread upon successful dequeue of a message.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param record The actual record body. This will be a byte array which is encoded in a manner specific to the pipeline. the record may be null or empty (0 length).
     */
    protected abstract void writeBinaryImpl(String sourceProvider, String sourceTopic, byte[] record);

    /**
     * This implementation of write extends the standard model to enqueue the record into a record queue for processing.
     * This call is syncrhonous in the caller, whereas handling of the message ultimately is done by a dedicated Executor thread via the run loop.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param records An Iterable&lt;byte[]&gt; of record byte arrays, behavior should be similar to sending each record individually.
     * @return
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        if (records!=null) {
            this.binaryBagMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, records));
            return true;
        }
        return false;
    }

    /**
     * Implementors implement this method to be called syncrhonously by the run loop Executor thread upon successful dequeue of a message.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param records An Iterable&lt;byte[]&gt; of record byte arrays, behavior should be similar to sending each record individually.
     */
    protected abstract void writeBinaryImpl(String sourceProvider, String sourceTopic, Iterable<byte[]> records);
}
