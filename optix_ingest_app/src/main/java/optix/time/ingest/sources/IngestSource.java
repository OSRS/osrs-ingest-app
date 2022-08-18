/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package optix.time.ingest.sources;

import optix.time.ingest.RunState;

/**
 * Interface from which all sources extend
 */
public interface IngestSource {
    /**
     * Initialize this IngestSource instance to be ready to begin ingesting.
     * This method is called at least once prior to any supported calls to start or stop.
     * Subsequent calls to initialize should not fail and should never raise an exception.
     * If the item fails to initialize it should transition to and return the RunState.Failed state.
     * @param name The name this IngestSource should have throughout the lifetime of the component.
     * @return The state of this IngestSource at the conclusion of initialization.
     */
    boolean initialize(String name);

    /**
     * Starts this IngestSource handling messages. If the source is in an unsuitable state to be started, this should have no effect.
     * This method is properly called when the RunState of the source is Initialized, Stopped, or Failed.
     * Subsequent calls to start should not fail and should never raise an exception.
     * If the item fails to start, it should transition to and return the RunState.Failed state.
     * If the items fails at any time while running it should transition to the RunState.Failed state, where it may be restarted if applicable.
     * @return The state of this IngestSource at the conclusion of starting.
     */
    boolean start();

    /**
     * Staops this IngestSource handling messages. If the source is in an unsuitable state to be stopped, this should have no effect.
     * This method is properly called when the RunState of the source is Running.
     * Subsequent calls to stop should not fail and should never raise an exception.
     * If the item fails to stop, it should transition to and return the RunState.Failed state.
     * @return The state of this IngestSource at the conclusion of stopping.
     */
    boolean stop();

    /**
     * Gets the current running state of the source
     * Note that while any method that results in the change of the state is executing, the first action should be to set the state to Transitioning.
     * The Transitioning state is an indicator that no other actions should be performed relative to this source until the current action completes.
     * It is expected that any method causing a transition between states is synchronized for safety based upon a shared lock object.
     * @return the current running state of the source
     */
    RunState getState();
}
