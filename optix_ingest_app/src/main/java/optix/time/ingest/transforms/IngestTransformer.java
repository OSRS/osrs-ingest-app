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
 * Interface for generic record types where a type String result will be returned
 * @param <F> - Type of the (from) record
 */
public interface IngestTransformer<F> extends RecordTransformer<F,String> {
    String transform(final String sourceProvider, final String sourceTopic, F record);
    Iterable<String> transform(final String sourceProvider, final String sourceTopic, Iterable<F> records);
}
