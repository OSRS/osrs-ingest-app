/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
import optix.time.ingest.LocalConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class LocalConfigTest {

    LocalConfig testConfig = null;

    @Before
    public void setUp() {
        // one-time initialization code
        // Use testing config
        this.testConfig = new LocalConfig("target/test-classes/config.json");
    }

    @Test
    public void LocalConfigFileNotNull() {
        Assert.assertNotNull(testConfig);
    }

    @Test
    public void LocalConfigGetDeploymentName() {
        testConfig.getWriters();
        Assert.assertEquals("junit_testing_app", testConfig.getDeploymentName());
    }

    @Test
    public void LocalConfigGetWriters() {
        HashMap<String, HashMap<String, String>> writers = testConfig.getWriters();
        Assert.assertEquals(2,writers.size());

        HashMap<String, String> writerConfig =  writers.get("output_mqtt");
        Assert.assertEquals(3,writerConfig.size());

        Assert.assertEquals(true, writerConfig.containsKey("host"));
        Assert.assertEquals(true, writerConfig.containsKey("port"));
        Assert.assertEquals(true, writerConfig.containsKey("type"));
    }

    @Test
    public void LocalConfigGetSources() {
        HashMap<String, HashMap<String, String>> sources = testConfig.getSources();
        Assert.assertEquals(2, sources.size());

        HashMap<String, String> sourceConfig =  sources.get("input_mqtt");
        Assert.assertEquals(3, sourceConfig.size());

        Assert.assertEquals(true, sourceConfig.containsKey("host"));
        Assert.assertEquals(true, sourceConfig.containsKey("port"));
        Assert.assertEquals(true, sourceConfig.containsKey("type"));
    }

    @Test
    public void LocalConfigGetTransformerTypes() {
        HashMap<String, String> transformerTypes = testConfig.getTransformerTypes();
        Assert.assertEquals(1, transformerTypes.size());
        Assert.assertEquals(true, transformerTypes.containsKey("lambda"));
    }

    @Test
    public void LocalConfigGetSourceTypes() {
        HashMap<String, String> sourceTypes = testConfig.getSourceTypes();
        Assert.assertEquals(1, sourceTypes.size());
        Assert.assertEquals(true, sourceTypes.containsKey("mqtt"));
    }

    @Test
    public void LocalConfigGeWriterTypes() {
        HashMap<String, String> writerTypes = testConfig.getWriterTypes();
        Assert.assertEquals(1, writerTypes.size());
        Assert.assertEquals(true, writerTypes.containsKey("mqtt"));
    }

}
