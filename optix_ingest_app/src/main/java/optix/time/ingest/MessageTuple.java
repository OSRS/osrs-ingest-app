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

/**
 * An abstraction of the message as a source, topic, record tuple.
 * @param <T> The record data type, generally string or byte[]
 */
public class MessageTuple<T> {
    public final String source;
    public final String topic;
    public final T record;

    public MessageTuple(String source, String topic, T record){
        this.source=source;
        this.topic=topic;
        this.record=record;
    }
}
