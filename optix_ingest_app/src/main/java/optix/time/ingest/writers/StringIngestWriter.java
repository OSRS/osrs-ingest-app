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

import java.util.Base64;
import java.util.Iterator;

/**
 * Abstract convenience class that can be implemented for writing records with string content
 */
public abstract class StringIngestWriter extends AbstractSimpleIngestWriter {
    private static final Base64.Encoder encoder = Base64.getEncoder();

    /**
     * byte to string conversion happens on write. The resulting converted record will be handled by the user defined
     * string write() call
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, byte[] record) {
        if (record!=null)
            return this.write(sourceProvider, sourceTopic, encoder.encodeToString(record));
        return this.write(sourceProvider, sourceTopic, (String)null);
    }

    /**
     * byte iterable case where each record will be converted to a string upon retrieval.
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        if (records!=null)
            return this.write(sourceProvider, sourceTopic, new Wrapper(records));
        return this.write(sourceProvider, sourceTopic, (Iterable<String>)null);
    }

    public final class Wrapper implements Iterable<String>{
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

        private Wrapper(Iterable<byte[]> records){
            this.inner=records;
        }
    }
}
