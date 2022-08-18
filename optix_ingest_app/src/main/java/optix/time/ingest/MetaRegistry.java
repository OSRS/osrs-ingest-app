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
 * Abstraction of a RouteTable Metadata Entry provider
 */
public interface MetaRegistry {
    /**
     * Initialize this registry to enable fetching.
     * @return boolean indicating if this registry successfully initialized
     */
    boolean initialize();

    /**
     * fetch the current set of meta entries from the registry
     * @return
     */
    Iterable<MetaEntry> fetchMetaEntries();
}
