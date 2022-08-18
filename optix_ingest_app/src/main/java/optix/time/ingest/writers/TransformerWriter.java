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

import com.amazonaws.services.dynamodbv2.xspec.B;
import optix.time.ingest.IngestConfiguration;
import optix.time.ingest.RunState;
import optix.time.ingest.transforms.RecordTransformer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;

/**
 * The TransformerWriter wraps a RecordTransformer. All messages received will be transformed if a transformer exists
 */
public final class TransformerWriter implements IngestWriter {
    private final static Log logger = LogFactory.getLog(TransformerWriter.class);
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Base64.Decoder decoder = Base64.getDecoder();

    /**
     * Factory method for creating TransformerWriters
     * @param transform - The transform TypeConfiguration. null indicates a no-op pass-through of the message.
     * @param transformInfo - The configuration associated with a transformer route. This is everything after the ':' in the route table
     * @param writer - The writer will receive the message to write after the transform occurs.
     * @param destProvider - The target destination provider
     * @param destTopic - The target destination topic
     * @param maxBatchSize - the maximum number of messages to send to the transformer in a single Iterable batch call
     * @return new TransformerWriter instance
     */
    public static TransformerWriter createTransform(final IngestConfiguration.TypeConfiguration transform, final String transformInfo, final IngestWriter writer, final String destProvider, final String destTopic, final int maxBatchSize){
        if (writer!=null && destProvider!=null && destTopic!=null && destProvider.length()>0){
            if (transform!=null) {
                try {
                    RecordTransformer<String, String> xfrm = transform.createRecordTransformer();
                    if (xfrm != null) {
                        if (xfrm.initialize(transformInfo)) {
                            return new TransformerWriter(xfrm, writer, destProvider, destTopic, maxBatchSize);
                        }
                    }
                } catch (Exception e) {
                }
            }
            return new TransformerWriter(null, writer, destProvider, destTopic, maxBatchSize);
        }
        return null;
    }

    /**
     * Note this doesn't actually need to do anything but set state.
     * The writer is initialized and started/stopped separately by the engine
     * The transformers don't start/stop, just initialize, which is performed on creation in the createTransform method
     * @param name
     * @return
     */
    @Override
    public boolean initialize(String name) {
        if (this.state == RunState.Created || this.state == RunState.FailedInitialization) {
            this.state = RunState.Initialized;
            return true;
        }
        this.state = RunState.FailedInitialization;
        return false;
    }

    /**
     * If the transformer is null, pass through the message to the writer
     * Otherwise transform the record and write the output of the transformation to the writer
     * @param sourceProvider - Record origin source provider
     * @param sourceTopic - Record origin source topic
     * @param record - The record in which to transform/write
     * @return true if write succeeded
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, String record) {
        if (this.xform!=null)
            return this.writer.write(destProvider, destTopic, this.xform.transform(sourceProvider, sourceTopic, record));
        return this.writer.write(destProvider, destTopic, record);
    }

    /**
     * Respecting the maxBatchSize, accumulate a subset of records and transform/write to the writer
     * @param sourceProvider - Record origin source provider
     * @param sourceTopic - Record origin source topic
     * @param records - List of records in which to transform/write
     * @return true if writes succeed
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, Iterable<String> records) {
        if (records!=null) {
            if (this.maxBatchSize > 0) {
                Iterator<String> it = records.iterator();
                Batcher<String> batch=new Batcher<>(it);
                Iterator<String> batchIter=batch.iterator();
                boolean status=true;
                while(batchIter.hasNext()){
                    ArrayList<String> tmp=new ArrayList<>(); //we have to realize the copy so it enqueues properly for threading

                    while(batchIter.hasNext())
                        tmp.add(batchIter.next());

                    if (this.xform != null)
                        status = status && this.writer.write(destProvider, destTopic, this.xform.transform(sourceProvider, sourceTopic, tmp));
                    else
                        status = status && this.writer.write(destProvider, destTopic, tmp);

                    batchIter = batch.iterator(); //for the next batch
                }
                return status;
            }else{
                if (this.xform != null)
                    return this.writer.write(destProvider, destTopic, this.xform.transform(sourceProvider, sourceTopic, records));
                return this.writer.write(destProvider, destTopic, records);
            }
        }
        return true; //no data is a successful write
    }

    /**
     * If the transformer is null, pass through the message to the writer
     * Otherwise transform the record and write the output of the transformation to the writer
     * @param sourceProvider - Record origin source provider
     * @param sourceTopic - Record origin source topic
     * @param record - The record in which to transform/write
     * @return true if write succeeded
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, byte[] record) {
        if (this.xform!=null) {
            if (record != null) {
                String s = this.xform.transform(sourceProvider, sourceTopic, encoder.encodeToString(record));
                if (s != null)
                    return this.writer.writeBinary(destProvider, destTopic, decoder.decode(s));
                return this.writer.writeBinary(destProvider, destTopic, (byte[]) null);
            }

            String s = this.xform.transform(sourceProvider, sourceTopic, (String) null);
            if (s != null)
                return this.writer.writeBinary(destProvider, destTopic, decoder.decode(s));
            return this.writer.writeBinary(destProvider, destTopic, (byte[]) null);
        }
        return this.writer.writeBinary(destProvider, destTopic, record);
    }

    /**
     * Respecting the maxBatchSize, accumulate a subset of records and transform/write to the writer
     * @param sourceProvider - Record origin source provider
     * @param sourceTopic - Record origin source topic
     * @param records - List of records in which to transform/write
     * @return true if writes succeed
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        if (records!=null) {
            Iterable<String> records2 = new EncodeWrapper(records); //do the base64 encoding per record
            //NOTE: the this.writer.writeBinary all use a companion DecodeWrapper(x) to undo the base64 encoding per record

            if (this.maxBatchSize > 0) {
                Iterator<String> it = records2.iterator();
                Batcher<String> batch=new Batcher<>(it);
                Iterator<String> batchIter=batch.iterator();
                boolean status=true;
                while(batchIter.hasNext()){
                    ArrayList<String> tmp=new ArrayList<>(); //we have to realize the copy so it enqueues properly for threading

                    while(batchIter.hasNext())
                        tmp.add(batchIter.next());

                    if (this.xform != null)
                        status = status && this.writer.writeBinary(destProvider, destTopic, new DecodeWrapper(this.xform.transform(sourceProvider, sourceTopic, tmp)));
                    else
                        status = status && this.writer.writeBinary(destProvider, destTopic, new DecodeWrapper(tmp));

                    batchIter=batch.iterator(); //for the next batch
                }
                return status;
            }else{
                if (this.xform != null)
                    return this.writer.writeBinary(destProvider, destTopic, new DecodeWrapper(this.xform.transform(sourceProvider, sourceTopic, records2)));
                return this.writer.writeBinary(destProvider, destTopic, new DecodeWrapper(records2));
            }
        }
        return true; //no data is a successful write
    }

    public boolean start(){
        if (this.state == RunState.Initialized || this.state == RunState.Stopped || this.state == RunState.Failed) {
            this.state = RunState.Running;
            return true;
        }
        return this.state!=RunState.Failed;
    }

    public boolean stop(){
        if (this.state == RunState.Running) {
            this.state = RunState.Stopped;
            return true;
        }
        return false;
    }

    private RunState state;
    @Override
    public RunState getState() {
        return this.writer.getState();
    }

    private final RecordTransformer<String, String> xform;
    private final IngestWriter writer;
    private final String destProvider;
    private final String destTopic;
    private final int maxBatchSize;

    private TransformerWriter(final RecordTransformer<String, String> xform, final IngestWriter writer, final String destProvider, final String destTopic, final int maxBatchSize){
        this.xform=xform;
        this.writer=writer;
        this.destProvider=destProvider;
        this.destTopic=destTopic;
        if (maxBatchSize>0)
            this.maxBatchSize=maxBatchSize;
        else
            this.maxBatchSize=0;
    }

    /**
     * this allows for batching an iterable without multiple scans - all lazily
     * @param <T>
     */
    private final class Batcher<T> implements Iterable<T>{
        private final Iterator<T> shrederator;
        private boolean innerNext;

        private Batcher(Iterator<T> inner){
            this.shrederator=inner;
            this.innerNext=shrederator.hasNext();
        }

        public Iterator<T> iterator(){return new WrapperIterator();}

        private final class WrapperIterator implements Iterator<T>{
            private int index=0;

            @Override
            public boolean hasNext() {
                return this.index < maxBatchSize && innerNext;
            }

            @Override
            public T next() {
                if (this.index<maxBatchSize && innerNext){
                    this.index++; //do it here so "hasNext" can be called forever
                    T tmp=shrederator.next();
                    innerNext=shrederator.hasNext(); //set up for the next time
                    return tmp;
                }
                return null; //to prevent overruns on an instance
            }

            private WrapperIterator(){
            }
        }
    }

    public final class EncodeWrapper implements Iterable<String>{
        private final Iterable<byte[]> inner;

        @Override
        public Iterator<String> iterator() {
            return new WrapperIterator(inner.iterator());
        }

        public final class WrapperIterator implements Iterator<String>{
            private final Iterator<byte[]> inneriter;

            @Override
            public boolean hasNext() {
                return this.inneriter.hasNext();
            }

            @Override
            public String next() {
                return encoder.encodeToString(this.inneriter.next());
            }

            private WrapperIterator(Iterator<byte[]> inneriter){
                this.inneriter=inneriter;
            }
        }

        private EncodeWrapper(Iterable<byte[]> records){
            this.inner=records;
        }
    }

    public final class DecodeWrapper implements Iterable<byte[]>{
        private final Iterable<String> inner;

        @Override
        public Iterator<byte[]> iterator() {
            return new WrapperIterator(inner.iterator());
        }

        public final class WrapperIterator implements Iterator<byte[]>{
            private final Iterator<String> inneriter;

            @Override
            public boolean hasNext() {
                return this.inneriter.hasNext();
            }

            @Override
            public byte[] next() {
                return decoder.decode(this.inneriter.next());
            }

            private WrapperIterator(Iterator<String> inneriter){
                this.inneriter=inneriter;
            }
        }

        private DecodeWrapper(Iterable<String> records){
            this.inner=records;
        }
    }
}
