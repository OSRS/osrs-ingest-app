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
 * Represents a single RouteTable entry metadata.
 * Composed of the items required to create a RouteTable entry:
 *   sourceProvider: the name of an IngestSource available via the IngestEngine.getSource(sourceProvider)
 *   sourceTopic: the name of the topic or wildcard match clause for selecting routes from the source. NOTE: order is not guaranteed and messages will be routed to the first matching clause.
 *   destProvider: the name of an IngestWriter available via the IngestEngine.getWriter(destProvider)
 *   destTopic:  the name of the topic for routing messages to. NOTE: all messages on this route will be written to this topic.
 *   maxBatchSize: the maximum number of messages to send to the transformer in a single Iterable batch call
 *   transformMeta: the formatted string of "name:config" for the transformer to use. This will be constructed by the IngestRouter. An example is "Lambda:myLambdaName" for a Lambda transformer calling the lambda named "myLambdaName"
 *   NOTE: the transformMeta can be null or string.empty to indicate a "null transform" which simply routes messages unchanged to the destination writer/topic.
 */
public final class MetaEntry {
    public final String sourceProvider;
    public final String sourceTopic;
    public final String destProvider;
    public final String destTopic;
    public final int maxBatchSize;
    public final String transformMeta;

    public MetaEntry(final String sourceProvider, final String sourceTopic, final String destProvider,
                     final String destTopic, final int maxBatchSize, final String transformMeta){
        this.sourceProvider=sourceProvider;
        this.sourceTopic=sourceTopic;
        this.destProvider=destProvider;
        this.destTopic=destTopic;
        this.maxBatchSize=maxBatchSize;
        this.transformMeta=transformMeta;
    }

    /**
     * @return True if this MetaEntry has a valid transformMeta
     */
    public boolean hasTransform(){
        return this.transformMeta!=null && this.transformMeta.length()>0;
    }

    /**
     * Gets the name from the transformMeta string which is all characters prior to the first ':' character
     * Example "lambda:myLambdaName" will return "lambda"
     * @return String - transform type name
     */
    public String getTransformTypeName(){
        if (this.hasTransform()) {
            int keyPt = transformMeta.indexOf(':');
            if (keyPt > 0) {
                return transformMeta.substring(0, keyPt).toLowerCase();
            }
        }
        return null;
    }

    /**
     * Gets the entire transformMeta string following the first ':' character
     * Example "lambda:myLambdaName" will return "myLambdaName"
     * @return String - transform config information
     */
    public String getTransformMeta(){
        if (this.hasTransform()) {
            int keyPt = transformMeta.indexOf(':');
            if (keyPt > 0) {
                return transformMeta.substring(keyPt + 1);
            }
        }
        return null;
    }

    @Override
    public String toString(){
        return String.format("sourceProvider = %s, sourceTopic = %s, destProvider = %s, destTopic = %s, maxBatchSize = %d, transformMeta = %s",
                sourceProvider, sourceTopic, destProvider, destTopic, maxBatchSize, transformMeta);
    }
}
