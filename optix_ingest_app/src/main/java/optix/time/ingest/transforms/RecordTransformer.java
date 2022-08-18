/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package optix.time.ingest.transforms;

/**
 * Generic interface that dictates how all transformers should behave
 * @param <F> - Type of the original (from) record
 * @param <T> - Return type of the transformation (to)
 */
public interface RecordTransformer<F,T> {
    boolean initialize(String info);
    T transform(final String sourceProvider, final String sourceTopic, F record);
    Iterable<T> transform(final String sourceProvider, final String sourceTopic, Iterable<F> records);
}
