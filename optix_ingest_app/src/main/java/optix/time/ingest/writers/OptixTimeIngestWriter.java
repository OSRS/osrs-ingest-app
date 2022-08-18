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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.Set;

/**
 * This class is a wrapper to MQTTIngestWriter and performs JSON validation on the message contents before sending the message
 * onwards to an MQTTIngestWriter instance.
 */
public final class OptixTimeIngestWriter extends AbstractIngestWriter{
    final static Log logger = LogFactory.getLog(OptixTimeIngestWriter.class);
    private static final Base64.Encoder encoder = Base64.getEncoder();

    private IngestWriter mqttIngestWriter;
    private JSONParser jsonParser = new JSONParser();

    /**
     * Verify that the record conforms to the Optix.Time specifications prior to sending it off to the writer.
     * @param sourceProvider - The provider we are sending messages to
     * @param sourceTopic - The topic the record will be writen to
     * @param record - The actual message being sent
     * @return true if the record was written successfully
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, String record) {
        if(validateJsonMessage(record)){
            //Minify json content as our internal mqtt broker expects all json on a single line
            try {
                String minifiedRecord = jsonParser.parse(record).toString();
                return mqttIngestWriter.writeBinary(sourceProvider, sourceTopic, minifiedRecord.getBytes(StandardCharsets.UTF_8));
            } catch (ParseException e) {
                logger.error("", e);
                return false;
            }
        }else{
            logger.warn("OptixTimeIngestWriter Invalid Message : " + record);
            return false;
        }
    }

    /**
     * For each record in records, Verify that the record conforms to the Optix.Time specifications prior to sending it off to the writer.
     * @param sourceProvider - The provider we are sending messages to
     * @param sourceTopic - The topic the records will be writen to
     * @param records - List of records to send
     * @return true if the records were written successfully
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, Iterable<String> records) {

        //We'll pass through all records that succeed validation as a single message
        StringBuilder minifiedRecords = new StringBuilder();

        for(String record : records){
            if(validateJsonMessage(record)){

                //Minify json content as our internal mqtt broker expects all json on a single line
                try {
                    String minifiedRecord = jsonParser.parse(record).toString();
                    minifiedRecords.append(minifiedRecord).append("\n");
                } catch (ParseException e) {
                    logger.error("", e);
                }

            }else{
                logger.warn("OptixTimeIngestWriter Invalid Message : " + record);
            }
        }

        return mqttIngestWriter.write(sourceProvider, sourceTopic, minifiedRecords.toString());
    }

    /**
     * The byte record coming in will be passed to the string write() call for json validation
     * @param sourceProvider - The provider we are sending messages to
     * @param sourceTopic - List of records to send
     * @param record - The actual message being sent
     * @return true if the record was written successfully
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, byte[] record) {
        if (record!=null)
            return this.write(sourceProvider, sourceTopic, new String(record, StandardCharsets.UTF_8));
        return true;
    }

    /**
     * The list of byte records coming in will be passed to the string write() call for json validation
     * @param sourceProvider - The provider we are sending messages to
     * @param sourceTopic - The topic the records will be writen to
     * @param records - List of records to send
     * @return true if the records were written successfully
     */
    @Override
    public boolean writeBinary(String sourceProvider, String sourceTopic, Iterable<byte[]> records) {
        if (records!=null) {
            for (byte[] x : records) {
                this.writeBinary(sourceProvider, sourceTopic, x);
            }
        }
        return true;
    }

    @Override
    protected void run() {
        logger.info("Starting OptixTimeIngestWriter");

        if(mqttIngestWriter.start()){
            logger.info("OptixTimeIngestWriter started");
            return;
        }

        this.setFailed();
    }

    /**
     * This user defined initialization routine will be called on initialize
     * @return
     */
    @Override
    protected boolean initImpl() {
        logger.info("Initializing OptixTimeIngestWriter");

        mqttIngestWriter = new MQTTIngestWriter();
        mqttIngestWriter.initialize(this.getName());

        if(mqttIngestWriter.getState() == RunState.Initialized){
            logger.info("OptixTimeIngestWriter initialized");
            return true;
        }else{
            logger.error("OptixTimeIngestWriter initialization failed");
        }
        return false;
    }

    /**
     * This user defined stop routine will be called on stop
     * @return
     */
    @Override
    protected boolean stopImpl() {
        logger.info("Stopping OptixTimeIngestWriter");

        if(mqttIngestWriter.stop()){
            logger.info("OptixTimeIngestWriter stopped");
            return true;
        }
        return false;
    }

    /**
     * Parse a given message and validate on the following criteria:
     * Must be valid json
     * Must adhere to the following structure
     * [{"metrics": [ {NOT_EMPTY}, ...], {"static": [ {NOT_EMPTY}, ...], {"dynamic": [ {NOT_EMPTY}, {"name":"timeStamp","value":"timestamp_str"}, ...] }, ..]
     * @param message - String of json to be validated
     * @return - True if message is valid
     */
    public boolean validateJsonMessage(final String message){

        if(message != null && !message.isEmpty()) {

            try {
                JSONArray root = (JSONArray) jsonParser.parse(message);

                boolean isValid = true;

                for (Object o : root) {
                    JSONObject jobj = (JSONObject) o;

                    if (((JSONArray) jobj.get("metrics")).isEmpty() ||
                            ((JSONArray) jobj.get("static")).isEmpty() ||
                            ((JSONArray) jobj.get("dynamic")).isEmpty()
                    ) {
                        isValid = false;
                        break;
                    } else {

                        //Ensure that each object in JSONArray contains "name" and "value"
                        if (!validJsonArrayContent((JSONArray) jobj.get("metrics")) ||
                                !validJsonArrayContent((JSONArray) jobj.get("static")) ||
                                !validJsonArrayContent((JSONArray) jobj.get("dynamic"))
                        ) {
                            isValid = false;
                            break;
                        }

                        //Check that a timestamp exists within dynamic content
                        JSONArray dynamicContent = (JSONArray) jobj.get("dynamic");

                        boolean foundTimestamp = false;
                        for (Object dynO : dynamicContent) {
                            JSONObject jDynO = (JSONObject) dynO;

                            if (jDynO.get("name").equals("timeStamp")) {
                                foundTimestamp = true;
                                break;
                            }
                        }

                        if (!foundTimestamp) {
                            isValid = false;
                            break;
                        }

                    }

                }

                return isValid;

            } catch (Exception e) {
                //Don't want to log this exception, an invalid json message should just be dropped
            }
        }

        return false;

    }

    private boolean validJsonArrayContent(JSONArray arr){
        boolean isValid = true;

        for(Object o : arr){
            JSONObject jobj = (JSONObject) o;

            Set keys = jobj.keySet();

            if(keys.size() != 2 ||
                    !keys.contains("name") ||
                    !keys.contains("value")
            ){
                isValid = false;
                break;
            }

        }

        return isValid;
    }

    public final class Wrapper implements Iterable<String>{
        private final Iterable<byte[]> inner;

        @Override
        public Iterator<String> iterator() {
            return null;
        }

        public final class WrapperIterator implements Iterator<String>{
            private final Iterator<byte[]> inneriter;

            @Override
            public boolean hasNext() {
                return this.inneriter.hasNext();
            }

            @Override
            public String next() {
                return encoder.encodeToString(this.inneriter.next());
            }

            private WrapperIterator(Iterator<byte[]> inneriter){
                this.inneriter=inneriter;
            }
        }

        private Wrapper(Iterable<byte[]> records){
            this.inner=records;
        }
    }
}
