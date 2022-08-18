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

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Abstract convenience class that can be implemented for writing records with byte content
 */
public abstract class BinaryIngestWriter extends AbstractSimpleIngestWriter {

    /**
     * String to byte conversion happens on write. The resulting converted record will be handled by the user defined
     * string write() call
     */
    public boolean write(final String sourceProvider, final String sourceTopic, final String record){
        return this.writeBinary(sourceProvider, sourceTopic, record.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * String iterable case where each record will be converted to a byte[] upon retrieval.
     */
    public boolean write(final String sourceProvider, final String sourceTopic, final Iterable<String> records){
        if (records!=null)
            return this.writeBinary(sourceProvider, sourceTopic, new Wrapper(records));
        return this.writeBinary(sourceProvider, sourceTopic, (Iterable<byte[]>)null);
    }

    public final class Wrapper implements Iterable<byte[]>{
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
                return this.inneriter.next().getBytes(StandardCharsets.UTF_8);
            }

            private WrapperIterator(Iterator<String> inneriter){
                this.inneriter=inneriter;
            }
        }

        private Wrapper(Iterable<String> records){
            this.inner=records;
        }
    }
}
