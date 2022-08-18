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
 * This RunState enumeration details the states in which a runtime component can be in and implementors will follow the base transition model as follows:
 *
 * Created -&gt; Initialized -&gt; Running -&gt; Stopped -&gt; Running
 *         |-&gt; FailedInitialization -&gt; Initialized -&gt; Running
 *                                  |-&gt; FailedInitialization
 *
 * Created -&gt; Initialized -&gt; Running -&gt; Stopped -&gt; Running
 *                        |-&gt; Failed -&gt; Running -&gt; Stopped
 *                                              |-&gt; Failed -&gt; Running
 *
 * Created -- initial state on object creation
 * Transitioning -- state that is initially visited when transitioning between any other states. This should be considered a non-shared (locked) state.
 * Initialized -- state visited from Created or FailedInitialization that indicates a successful initialization
 * Running -- state visited from Initialized, Stopped or Failed that indicates the service is currently operating normally doing any work
 * Stopped -- state visited from Running that indicates the service is currently doing no work but was successfully initialized and had previously been running. Can be started again.
 * Failed -- state visited from Running that indicates the service has failed while running and is no longer functioning. Can be started again, but may again fail.
 * FailedInitialization -- state visited from Created that indicates a failure while attempting initialization. Can be initialized again, but may again fail.
 */
public enum RunState {
    /**
     * initial state on object creation
     */
    Created,

    /**
     * state visited from Created or FailedInitialization that indicates a successful initialization
     */
    Initialized,

    /**
     * state visited from Initialized, Stopped or Failed that indicates the service is currently operating normally doing any work
     */
    Running,

    /**
     * state visited from Running that indicates the service is currently doing no work but was successfully initialized and had previously been running. Can be started again.
     */
    Stopped,

    /**
     * state visited from Running that indicates the service has failed while running and is no longer functioning. Can be started again, but may again fail.
     */
    Failed,

    /**
     * state visited from Created that indicates a failure while attempting initialization. Can be initialized again, but may again fail.
     */
    FailedInitialization,

    /**
     * state that is initially visited when transitioning between any other states. This should be considered a non-shared (locked) state.
     */
    Transitioning
}
