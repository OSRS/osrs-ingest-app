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

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The WorkPool is a collection of concurrent queues which enqueues String, Iterable&lt;String&gt;, byte[]
 * and Iterable&lt;byte[]&gt; messages.
 *
 * This is used throughout the GIA as an efficient means of message handling that allows processing to occur at a later time.
 */
public final class WorkPool {
    public final ConcurrentLinkedQueue<MessageTuple<String>> stringMessages=new ConcurrentLinkedQueue<>();
    public final ConcurrentLinkedQueue<MessageTuple<byte[]>> binaryMessages=new ConcurrentLinkedQueue<>();
    public final ConcurrentLinkedQueue<MessageTuple<Iterable<String>>> stringBagMessages=new ConcurrentLinkedQueue<>();
    public final ConcurrentLinkedQueue<MessageTuple<Iterable<byte[]>>> binaryBagMessages=new ConcurrentLinkedQueue<>();

    /**
     * Enqueue String messages
     */
    public boolean write(String sourceProvider, String sourceTopic, String record) {
        if (record!=null) {
            this.stringMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, record));
            return true;
        }
        return false;
    }

    /**
     * Enqueue Iterable&lt;String&gt; messages
     */
    public boolean write(String sourceProvider, String sourceTopic, Iterable<String> records) {
        if (records!=null) {
            this.stringBagMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, records));
            return true;
        }
        return false;
    }

    /**
     * Enqueue byte[] messages
     */
    public boolean writeBinary(String sourceProvider, String sourceTopic, byte[] record) {
        if (record!=null) {
            this.binaryMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, record));
            return true;
        }
        return false;
    }

    /**
     * Enqueue Iterable&lt;byte[]&gt; messages
     */
    public boolean writeBinary(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        if (records!=null) {
            this.binaryBagMessages.offer(new MessageTuple<>(sourceProvider, sourceTopic, records));
            return true;
        }
        return false;
    }
}
