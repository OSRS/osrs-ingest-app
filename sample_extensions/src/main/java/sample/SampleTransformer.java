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

import optix.time.ingest.transforms.StringTransformer;

import java.util.Iterator;

/**
 * This class is a simple implementation of a StringTransformer that will reverse any provided message text.
 * Note that this is a standard implementation strategy where the transform function performs the transform on a single record or an Iterable.
 * Note that the StringTransformer abstract class handles the UTF-8 encoding / decoding for binary records paths.
 * Note that this class has an inner class that serves as an Iterable wrapper for the iterable call that performs a lazy evaluation. This approach is efficient, but may not be suitable for some situations.
 */
public class SampleTransformer implements StringTransformer {
    private String prefix;

    @Override
    public boolean initialize(String info) {
        if (info !=null && info.length()>0)
            this.prefix = info;
        return true;
    }

    @Override
    public String transform(String sourceProvider, String sourceTopic, String record) {
        if (record!=null && record.length()>0){
            char[] chars = record.toCharArray();
            for(int i=0;i<chars.length/2;i++){
                char t = chars[i];
                chars[i]=chars[chars.length-(i+1)];
                chars[chars.length-(i+1)]=t;
            }
            record=new String(chars);
        }
        if (prefix!=null)
            return prefix+"-"+record;
        return record;
    }

    @Override
    public Iterable<String> transform(String sourceProvider, String sourceTopic, Iterable<String> records) {
        if (records!=null){
            return new Wrapper(records);
        }
        return null;
    }

    public final class Wrapper implements Iterable<String>{
        private final Iterable<String> inner;

        @Override
        public Iterator<String> iterator() {
            return new WrapperIterator(inner.iterator());
        }

        public final class WrapperIterator implements Iterator<String>{
            private final Iterator<String> inneriter;

            @Override
            public boolean hasNext() {
                return this.inneriter.hasNext();
            }

            @Override
            public String next() {
                return transform(null, null, this.inneriter.next());
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
