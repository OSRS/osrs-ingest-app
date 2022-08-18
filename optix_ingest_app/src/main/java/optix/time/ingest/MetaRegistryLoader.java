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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a MetaRegistry based on the AWS Lambda REST endpoint - uses Lambda calls rather than HTTP direct ones
 * NOTE: a todo is to externalize this via reflection loading to allow for alternative registry implementations
 */
public class MetaRegistryLoader implements MetaRegistry{

    final static Log logger = LogFactory.getLog(MetaRegistryLoader.class);

    @Override
    public boolean initialize() {
        return true;
    }

    @Override
    public Iterable<MetaEntry> fetchMetaEntries() {
        return fetchImpl(); //for prod

        //Local testing
        //ArrayList<MetaEntry> it =new ArrayList<>();

        //For local MQTT testing
        //it.add(new MetaEntry("mqtt_in", "data/in", "mqtt_out", "data/out", 4, null ));

        /* example of a manual equivalent set of routes

        it.add(new MetaEntry("mqtt_in_test", "topic/data5", "internal_mqtt", "topic/out", 4, "lambda:optix_time_transform_sample"));
        it.add(new MetaEntry("sample", "topic/data5", "internal_mqtt", "topic/out", 4, "lambda:optix_time_transform_sample"));


        //Mac's test for sample extensions
        it.add(new MetaEntry("sample_source_test", "topic/data5", "sample_writer_test", "cat", 4, null));
        it.add(new MetaEntry("sample_source_test", "topic/data7", "sample_writer_test", "cat", 4, "reverser:\"backwards\""));
        it.add(new MetaEntry("sample_source_test", "foobar", "wrapper", "cat", 4, null));
        it.add(new MetaEntry("wrapper", "cat", "sample_writer_test", "dog", 4, null));
        */

        //return it;
    }

    /**
     * Performed the Lambda call to get meta entries
     * @return Iterable<MetaEntry> MetaEntryList - List containing retrieved meta entries from AWS Lambda call.
     */
    private Iterable<MetaEntry> fetchImpl() {

        // Call AWS Lambda for entry info
        // Code modified from https://medium.com/@joshua.a.kahn/invoking-an-aws-lambda-function-from-java-29efe3a03fe8
        Regions region = Regions.fromName("us-east-1");
        AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard()
                .withRegion(region);

        // No need to insert credentials if running on an EC2 instance
        // If running on local machine be sure to set ENV variable AWS_PROFILE=my_profile for the account
        // which houses the lambda.
        AWSLambda client = builder.build();

        // Payload to send to lambda
        JSONObject payloadToLambda = new JSONObject();
        payloadToLambda.put("deployment_name", IngestEngine.getInstance().getDeploymentName());

        // Construct the request to this lambda
        InvokeRequest req = new InvokeRequest()
                .withFunctionName("get-route-config")
                .withPayload(payloadToLambda.toString());

        // This call is synchronous and will block
        InvokeResult result = client.invoke(req);
        String responsePayload = new String(result.getPayload().array());

        //Parse the response
        JSONParser jsonParser = new JSONParser();
        try {
            JSONObject responseRoot = (JSONObject) jsonParser.parse(responsePayload);

            //Check for errors in the response message
            if(responseRoot.containsKey("errorMessage")){
                logger.error("Exception in lambda response:\n" +
                        "Error Type: " + responseRoot.get("errorMessage") + "\n" +
                        "Error Message: " + responseRoot.get("errorMessage"));
            }else{

                if(responseRoot.isEmpty()){
                    logger.error("Response from get-route-config lambda is empty");
                    return new ArrayList<MetaEntry>();
                }else{
                    //Create MetaEntry objects and return back List
                    return jsonToMetaEntryList(responseRoot);
                }

            }
        } catch (ParseException e) {
            logger.error("", e);
        }


        return null;
    }

    /**
     * Perform validation and convert lambda response json into a List of MetaEntry objects
     * @param root - top level json object from lambda response
     * @return List<MetaEntry> - List of MetaEntry objects
     */
    private List<MetaEntry> jsonToMetaEntryList(JSONObject root){

        List<MetaEntry> metaList = new ArrayList<>();

        //Top level keys are source names
        for(Object sourceObject : root.keySet()){

            //Fields needed for MetaEntry creation
            String sourceProvider = (String) sourceObject;
            String sourceTopic = "";
            String destProvider = "";
            String destTopic = "";
            int maxBatchSize = 0;
            String transformMeta = "";

            JSONObject sourceJsonObject = (JSONObject) root.get(sourceProvider);

            //2nd level are various topics
            for(Object topicObject : sourceJsonObject.keySet()){
                sourceTopic = (String) topicObject;

                JSONObject topicJsonObject = (JSONObject) sourceJsonObject.get(sourceTopic);

                //3rd level is config for a particular topic
                //These key names are known
                destProvider = (String) topicJsonObject.get("destName");
                destTopic = (String) topicJsonObject.get("destTopic");
                maxBatchSize = (int) (long) topicJsonObject.get("batchSize");
                transformMeta = (String) topicJsonObject.get("xformName");

                //Todo - Perform validation on fields
                //sourceProvider, only alphanumeric
                //sourceTopic, contains mqtt valid characters with an optional trailing /*
                //destProvider, only alphanumeric
                //destTopic, valid mqtt topic
                //maxBatchSize >= 1 ?
                //transformMeta, transformerType:transformerMetaInformation
                //note that even though some sources and destinations are not MQTT, we will still be leveraging
                //mqtt validation at this point

                metaList.add(new MetaEntry(sourceProvider, sourceTopic, destProvider, destTopic, maxBatchSize, transformMeta));

            }

        }

        return metaList;
    }
}
