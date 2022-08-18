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

import optix.time.ingest.RunState;

/**
 * Interface from which all writers extend
 */
public interface IngestWriter {
    /**
     * Initialize this IngestWriter instance to be ready to begin ingesting.
     * This method is called at least once prior to any supported calls to start or stop.
     * Subsequent calls to initialize should not fail and should never raise an exception.
     * If the item fails to initialize it should transition to and return the RunState.Failed state.
     * @param name The name this IngestWriter should have throughout the lifetime of the component.
     * @return The state of this IngestWriter at the conclusion of initialization.
     */
    boolean initialize(final String name);

    /**
     * Write a single string record from the sourceProvider with the sourceTopic to this writer destination.
     * Note that any implementor will have specific understanding of the actual format of the string records.
     * Note also that the implementor should anticipate null or empty length messages and handle them appropriately.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param record The actual record body. This will be a string which is encoded in a manner specific to the pipeline. the record may be null or empty (0 length).
     * @return true if the write succeeds, false otherwise. The return of true indicates this writer successfully handled the record, not necessarily that it was specifically written to an output.
     */
    boolean write(final String sourceProvider, final String sourceTopic, final String record);

    /**
     * Write an iterable of string records from the sourceProvider with the sourceTopic to this writer destination.
     * Note that any implementor will have specific understanding of the actual format of the string records.
     * Note also that the implementor should anticipate null or empty length messages and handle them appropriately.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param records An Iterable&lt;String&gt; of record strings, behavior should be similar to sending each record individually.
     * @return true if the aggregate write succeeds, false otherwise. The return of true indicates this writer successfully handled the records, not necessarily that any were specifically written to an output.
     */
    boolean write(final String sourceProvider, final String sourceTopic, final Iterable<String> records);

    /**
     * Write a single binary record from the sourceProvider with the sourceTopic to this writer destination.
     * Note that any implementor will have specific understanding of the actual format of the binary records.
     * Note also that the implementor should anticipate null or empty length messages and handle them appropriately.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param record The actual record body. This will be a byte array which is encoded in a manner specific to the pipeline. the record may be null or empty (0 length).
     * @return true if the write succeeds, false otherwise. The return of true indicates this writer successfully handled the record, not necessarily that it was specifically written to an output.
     */
    boolean writeBinary(final String sourceProvider, final String sourceTopic, final byte[] record);

    /**
     * Write an iterable of binary records from the sourceProvider with the sourceTopic to this writer destination.
     * Note that any implementor will have specific understanding of the actual format of the binary records.
     * Note also that the implementor should anticipate null or empty length messages and handle them appropriately.
     * @param sourceProvider The name of the sourceProvider that this record comes from.
     * @param sourceTopic The name of the sourceTopic from which this record originates at the sourceProvider.
     * @param records An Iterable&lt;byte[]&gt; of record byte arrays, behavior should be similar to sending each record individually.
     * @return true if the aggregate write succeeds, false otherwise. The return of true indicates this writer successfully handled the records, not necessarily that any were specifically written to an output.
     */
    boolean writeBinary(final String sourceProvider, final String sourceTopic, final Iterable<byte[]> records);

    /**
     * Starts this IngestWriter handling messages. If the writer is in an unsuitable state to be started, this should have no effect.
     * This method is properly called when the RunState of the writer is Initialized, Stopped, or Failed.
     * Subsequent calls to start should not fail and should never raise an exception.
     * If the item fails to start, it should transition to and return the RunState.Failed state.
     * If the items fails at any time while running it should transition to the RunState.Failed state, where it may be restarted if applicable.
     * @return The state of this IngestWriter at the conclusion of starting.
     */
    boolean start();

    /**
     * Staops this IngestWriter handling messages. If the writer is in an unsuitable state to be stopped, this should have no effect.
     * This method is properly called when the RunState of the writer is Running.
     * Subsequent calls to stop should not fail and should never raise an exception.
     * If the item fails to stop, it should transition to and return the RunState.Failed state.
     * @return The state of this IngestWriter at the conclusion of stopping.
     */
    boolean stop();

    /**
     * Gets the current running state of the writer
     * Note that while any method that results in the change of the state is executing, the first action should be to set the state to Transitioning.
     * The Transitioning state is an indicator that no other actions should be performed relative to this writer until the current action completes.
     * It is expected that any method causing a transition between states is synchronized for safety based upon a shared lock object.
     * @return the current running state of the writer
     */
    RunState getState();
}
