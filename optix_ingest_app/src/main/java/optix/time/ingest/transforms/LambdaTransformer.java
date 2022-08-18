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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;

/**
 * The LambdaTransformer sends message payload to an AWS lambda for processing. The transformed message is the lambda
 * response.
 *
 * AWS credentials are not implemented within this class. It is assumed that credentials are obtained from the running environment
 *
 * If running on a configured EC2 instance, set IAM role / policies with allowed lambda execution.
 *
 * If running on a machine outside of the AWS ecosystem, set the AWS_PROFILE environment variable prior to running the GIA.
 * The AWS_PROFILE requires a valid AWS profile configured on the machine.
 */
public final class LambdaTransformer implements StringTransformer{

    final static Log logger = LogFactory.getLog(StringTransformer.class);

    private String lambdaName;
    private AWSLambda client;
    private JSONParser jsonParser;

    /**
     * Establish an AWS client connection
     * @param info - the meta information associated with this transformer. In this case, the full string is the lambda
     *             function to call.
     * @return true if initialize was successful
     */
    @Override
    public boolean initialize(String info) {
        logger.info("Initializing LambdaTransformer for : " + info);
        if (info!=null && info.length()>0) {
            this.lambdaName=info;

            // Call AWS Lambda for entry info
            // Code modified from https://medium.com/@joshua.a.kahn/invoking-an-aws-lambda-function-from-java-29efe3a03fe8
            Regions region = Regions.fromName("us-east-1");
            AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard()
                    .withRegion(region);

            // No need to insert credentials if running on an EC2 instance
            // If running on local machine be sure to set ENV variable AWS_PROFILE=my_profile for the account
            // which houses the lambda.
            client = builder.build();

            jsonParser = new JSONParser();

            logger.info("LambdaTransformer for : " + info + " initialized");
            return true;
        }
        return false;
    }

    /**
     * Transform the provided record by sending it as a payload to the AWS lambda.
     * The record is wrapped in a json structure where the root element is "msg".
     * @param sourceProvider - source origin provider
     * @param sourceTopic - source origin topic
     * @param record - record to be transformed
     * @return String transformedRecord - the response payload of the lambda call
     */
    @Override
    public String transform(final String sourceProvider, final String sourceTopic, String record) {

        if (lambdaName!=null){
            logger.debug(String.format("%s - Performing Transform\n", lambdaName));
            logger.debug(String.format("Record in: %s\n", record));

            // Payload to send to lambda
            JSONObject payloadToLambda = new JSONObject();
            payloadToLambda.put("msg", record);

            // Construct the request to this lambda
            InvokeRequest req = new InvokeRequest()
                    .withFunctionName(this.lambdaName)
                    .withPayload(payloadToLambda.toString());

            // This call is synchronous and will block
            InvokeResult result = client.invoke(req);
            String responsePayload = new String(result.getPayload().array());

            //Parse the response
            String transformedRecord = parseLambdaResponse(responsePayload);
            return transformedRecord;
        }
        return null;

    }

    /**
     * Transform the provided records by sending them as a payload to the AWS lambda.
     * The records are wrapped in a json structure where the root element is "messages".
     * @param sourceProvider - source origin provider
     * @param sourceTopic - source origin topic
     * @param records - records to be transformed
     * @return Iterable&lt;String&gt; of records where each record was transformed
     */
    @Override
    public Iterable<String> transform(final String sourceProvider, final String sourceTopic, Iterable<String> records) {

        if (lambdaName!=null){
            logger.debug(String.format("%s - Performing Transform\n", lambdaName));

            //Convert records into json structure for lambda
            JSONObject payloadToLambda = new JSONObject();

            JSONArray recordArr = new JSONArray();
            for(String record : records){
                recordArr.add(record);
            }
            payloadToLambda.put("messages", recordArr);

            // Construct the request to this lambda
            InvokeRequest req = new InvokeRequest()
                    .withFunctionName(this.lambdaName)
                    .withPayload(payloadToLambda.toString());

            // This call is synchronous and will block
            InvokeResult result = client.invoke(req);
            String responsePayload = new String(result.getPayload().array());

            //Parse the response
            return parseLambdaResponseItr(responsePayload);

        }
        return null;

    }

    private String parseLambdaResponse(String responsePayload){

        try {
            JSONObject responseRoot = (JSONObject) jsonParser.parse(responsePayload);

            //Check for errors in the response message
            if (responseRoot.containsKey("errorMessage")) {
                logger.error("Exception in lambda response:\n" +
                        "Error Type: " + responseRoot.get("errorMessage") + "\n" +
                        "Error Message: " + responseRoot.get("errorMessage"));
            } else {

                if (responseRoot.isEmpty()) {
                    logger.error(String.format("Response from %s lambda is empty", this.lambdaName));
                } else {
                    String transformedRecord = responseRoot.get("msg").toString();
                    logger.debug(String.format("Out record: %s\n", transformedRecord));
                    return transformedRecord;
                }

            }
        }catch(Exception e){
            logger.error("Error processing lambda response", e);
            return null;
        }

        return null;
    }

    private ArrayList<String> parseLambdaResponseItr(String responsePayload){

        try {
            JSONObject responseRoot = (JSONObject) jsonParser.parse(responsePayload);

            //Check for errors in the response message
            if (responseRoot.containsKey("errorMessage")) {
                logger.error("Exception in lambda response:\n" +
                        "Error Type: " + responseRoot.get("errorMessage") + "\n" +
                        "Error Message: " + responseRoot.get("errorMessage"));
            } else {

                if (responseRoot.isEmpty()) {
                    logger.error(String.format("Response from %s lambda is empty", this.lambdaName));
                } else {
                    ArrayList<String> transformedRecords = new ArrayList<>();
                    JSONArray transformedRecordsJArr = (JSONArray) responseRoot.get("messages");

                    for(Object o : transformedRecordsJArr){
                        transformedRecords.add((String) o);
                    }

                    return transformedRecords;
                }

            }
        }catch(Exception e){
            logger.error("Error processing lambda response", e);
            return null;
        }

        return null;
    }

    public LambdaTransformer(){}
}
