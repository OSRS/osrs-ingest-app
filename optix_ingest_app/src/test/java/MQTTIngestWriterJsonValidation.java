/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
import optix.time.ingest.writers.OptixTimeIngestWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A good effort at coming up with different permutations of valid / invalid messages.
 */
public class MQTTIngestWriterJsonValidation {

    OptixTimeIngestWriter t = null;

    @Before
    public void setUp() {
        // one-time initialization code
        t = new OptixTimeIngestWriter();
    }

    @Test
    public void InvalidJSONString() {
        Assert.assertFalse(t.validateJsonMessage("This is not JSON"));
    }

    @Test
    public void ValidateOneMetricOneTimestamp() {
        Assert.assertTrue(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void ValidateTwoMetricsOneTimestamp() {
        Assert.assertTrue(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"},{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void ValidateTwoMetricsTwoTimestamps() {
        Assert.assertTrue(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"},{\"name\":\"pressure\",\"value\":\"13.5\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]},{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"},{\"name\":\"pressure\",\"value\":\"13.5\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void InvalidMessageNoTimestampSingleMetric() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"notTimeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void InvalidMessageNoTimestampMultipleMetrics() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"notTimeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]},{\"metrics\":[{\"name\":\"humidity\",\"value\":\"99.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"notTimeStamp\",\"value\":\"2017-11-30T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void InvalidMessageNoTimestampForSomeMetrics() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]},{\"metrics\":[{\"name\":\"humidity\",\"value\":\"99.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"notTimeStamp\",\"value\":\"2017-11-30T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void MissingMetricsObject() {
        Assert.assertFalse(t.validateJsonMessage("[{\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void MissingStaticObject() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void MissingDynamicObject() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}]}]"));
    }

    @Test
    public void InvalidContentMetrics() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"invalid\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void InvalidContentStatic() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"invalid\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]}]"));
    }

    @Test
    public void InvalidContentDynamic() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"},{\"invalid\":\"test\",\"value\":\"test\"}]}]"));
    }

    @Test
    public void InvalidContentCompound() {
        Assert.assertFalse(t.validateJsonMessage("[{\"metrics\":[{\"invalid\":\"humidity\",\"value\":\"28.1797\"}],\"static\":[{\"name\":\"address\",\"invalid\":\"00:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"},{\"invalid\":\"test\",\"value\":\"test\"}]}]"));
    }

    @Test
    public void validContentCompound() {
        Assert.assertTrue(t.validateJsonMessage("[{\"metrics\":[{\"name\":\"pressure\",\"value\":\"13.5\"},{\"name\":\"temperature\",\"value\":\"99\"}],\"static\":[{\"name\":\"address\",\"value\":\"01:a0:50:06:21:16\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"}]},{\"metrics\":[{\"name\":\"pressure\",\"value\":\"13.5\"}],\"static\":[{\"name\":\"address\",\"value\":\"00:a0:50:06:21:16\"},{\"name\":\"anotherTag\",\"value\":\"tagValue\"}],\"dynamic\":[{\"name\":\"timeStamp\",\"value\":\"2017-11-29T15:54:03.697730+00:00\"},{\"name\":\"extraDynamicContent\",\"value\":\"extra\"}]}]"));
    }

}
