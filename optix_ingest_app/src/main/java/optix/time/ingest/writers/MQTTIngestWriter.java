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

import optix.time.ingest.IngestConfiguration;
import optix.time.ingest.MessageTuple;
import optix.time.ingest.RunState;
import optix.time.ingest.utils.SSLConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * IngestWriter which connects and writes to an MQTT broker asynchronously. Connection supports basic tcp, or ssl with the following config options set:
 *   "sslEnabled": "true",
 *   "caCrtFile" : "/path/to/my.ca",
 *   "crtFile": "/path/to/my.cer",
 *   "keyFile": "/path/to/my.key"
 *
 * Additionally, a username and password can be set in the configuration to use with either tcp or ssl
 *   "username": "myuser",
 *   "password": "mypassword",
 *
 * Messages received will be written to the specified topic via a WorkPool allowing for optimal throughput when calling
 * the write() methods.
 */
public final class MQTTIngestWriter extends AbstractIngestWriter{
    final static Log logger = LogFactory.getLog(MQTTIngestWriter.class);
    private final WorkPool pool = new WorkPool();

    private String host = "";
    private String port = "";

    private MqttAsyncClient client;

    //Default values of optional config parameters
    private int qualityOfService = 0;
    private boolean cleanSession = true;
    private boolean sslEnabled = false;
    private boolean usernameAndPasswordEnabled = false;

    //Optional authentication parameters
    private String caCrtFile = "";
    private String crtFile = "";
    private String keyFile = "";
    private String username = "";
    private String password = "";

    /**
     * This is the initialization method which actually does the work.
     * @return
     */
    @Override
    protected boolean initImpl() {
        logger.info("Initializing MQTTIngestWriter");
        IngestConfiguration.WriterConfiguration cfg = IngestConfiguration.getInstance().getWriter(this.getName());
        if (cfg != null) {
            //Gather required config items
            boolean requiredConfigError = false;

            if (cfg.containsKey("host")) {
                host = cfg.get("host");
            }else{
                logger.error(this.getName() + " does not contain 'host' in config");
                requiredConfigError = true;
            }

            if (cfg.containsKey("port")) {
                port = cfg.get("port");
            }else{
                logger.error(this.getName() + " does not contain 'port' in config");
                requiredConfigError = true;
            }

            if(!requiredConfigError){
                //Get optional config values
                if(cfg.containsKey("qos") && qosIsValid(cfg.get("qos"))){
                    qualityOfService = Integer.parseInt(cfg.get("qos"));
                }

                if(cfg.containsKey("cleanSession")){
                    cleanSession = Boolean.valueOf(cfg.get("cleanSession"));
                }

                //Authentication mechanisms
                if(cfg.containsKey("sslEnabled")) {
                    sslEnabled = Boolean.valueOf(cfg.get("sslEnabled"));

                    if (sslEnabled) {

                        //caCrtFile is optional. If not provided no CA validation will be performed when establishing the SSL connection.
                        if (cfg.containsKey("caCrtFile")) {
                            caCrtFile = cfg.get("caCrtFile");
                        }

                        //The following are now required parameters since sslEnabled is true
                        if (cfg.containsKey("crtFile")) {
                            crtFile = cfg.get("crtFile");
                        } else {
                            logger.error(this.getName() + " does not contain 'crtFile' in config. Required when 'sslEnabled' is true");
                            return false;
                        }

                        if (cfg.containsKey("keyFile")) {
                            keyFile = cfg.get("keyFile");
                        } else {
                            logger.error(this.getName() + " does not contain 'keyFile' in config. Required when 'sslEnabled' is true");
                            return false;
                        }
                    }
                }

                if(cfg.containsKey("username")){

                    usernameAndPasswordEnabled = true;
                    username = cfg.get("username");

                    if(cfg.containsKey("password")){
                        password = cfg.get("password");
                    }else{
                        logger.warn(this.getName() + " does not contain 'password' in config.");
                    }
                }

                logger.info("MQTTIngestWriter initialized");
                return true;
            }
        }else{
            logger.error("Unable to get " + this.getName() + " writer");
        }
        return false;
    }

    /**
     * Quality of Service (qos) is a MQTT broker configuration option. This method checks if the qos string is a valid value.
     * qos can be 0, 1, or 2
     * @param qosVal - qos string to check
     * @return true if qos is valid
     */
    private boolean qosIsValid(String qosVal){
        try{
            int qos = Integer.parseInt(qosVal);
            return (qos >=0 && qos <= 2);
        }catch(Exception e){
            return false;
        }
    }

    /**
     * Write a String record into the WorkPool for future processing
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, String record) {
        logger.debug(String.format("%s writing: topic: %s\nmessage: %s", this.getName(), sourceTopic, record));
        return pool.write(sourceProvider, sourceTopic, record);
    }

    /**
     * Write String records into the WorkPool for future processing
     */
    @Override
    public boolean write(String sourceProvider, String sourceTopic, Iterable<String> records) {
        logger.debug(String.format("%s writing: topic: %s\nmessage: %s", this.getName(), sourceTopic, records));
        return pool.write(sourceProvider, sourceTopic, records);
    }

    /**
     * Write a byte[] record into the WorkPool for future processing
     */
    @Override
    public boolean writeBinary(final String sourceProvider, final String sourceTopic, final byte[] record){
        logger.debug(String.format("%s writing: topic: %s\nmessage: %s", this.getName(), sourceTopic, record));
        return pool.writeBinary(sourceProvider, sourceTopic, record);
    }

    /**
     * Write byte[] records into the WorkPool for future processing
     */
    @Override
    public boolean writeBinary(final String sourceProvider, final String sourceTopic, final Iterable<byte[]> records){
        logger.debug(String.format("%s writing: topic: %s\nmessage: %s", this.getName(), sourceTopic, records));
        return pool.writeBinary(sourceProvider, sourceTopic, records);
    }

    //From a given byte array, construct an MqttMessage
    private MqttMessage constructMessage(byte[] data){
        if (data!=null) {
            //Construct mqtt payload
            MqttMessage mqttPayload = new MqttMessage();
            mqttPayload.setPayload(data);
            mqttPayload.setQos(qualityOfService);
            return mqttPayload;
        }
        return null;
    }

    //Send a mqtt message to the specified topic
    private void send(String topic, MqttMessage mqttPayload){
        if (mqttPayload!=null) {
            logger.debug(String.format("%s sending: topic: %s", this.getName(), topic));
            try {
                client.publish(topic, mqttPayload);
            } catch (MqttException e) {
                logger.error("send", e);
            }
        }
    }

    private void writeAll(String topic, Iterable<String> records){
        for(String record : records){
            this.send(topic, constructMessage(record.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void writeAllRaw(String topic, Iterable<byte[]> records){
        for(byte[] record : records){
            this.send(topic, constructMessage(record));
        }
    }

    //ingest pulls work from the WorkPool and sends out each message
    private void ingest(){
        int exCount=0; //count of repeated exceptions
        while(this.getState() == RunState.Running){
            try {
                MessageTuple<String> a = this.pool.stringMessages.poll();
                if (a != null) {
                    this.send(a.topic, constructMessage(a.record.getBytes(StandardCharsets.UTF_8)));
                }

                MessageTuple<byte[]> b = this.pool.binaryMessages.poll();
                if (b != null) {
                    this.send(b.topic, constructMessage(b.record));
                }

                MessageTuple<Iterable<String>> c = this.pool.stringBagMessages.poll();
                if (c != null) {
                    this.writeAll(c.topic, c.record);
                }

                MessageTuple<Iterable<byte[]>> d = this.pool.binaryBagMessages.poll();
                if (d != null) {
                    this.writeAllRaw(d.topic, d.record);
                }
                exCount=0; //reset since we didn't fail this time round
            }catch (Exception e){exCount++;}
        }
    }

    /**
     * Connect to the MQTT broker and start the ingest WorkPool
     */
    @Override
    protected void run() {
        logger.info("Starting MQTTIngestWriter");
        //Attempt to connect to MQTT Broker
        try{
            String connectType = "tcp";
            if(sslEnabled){
                connectType = "ssl";
            }

            String connectionURI = String.format("%s://%s:%s", connectType, host, port);

            //Use the MQTT async client to not block on message publish
            client = new MqttAsyncClient(connectionURI, IngestConfiguration.getInstance().getName(), new MemoryPersistence());
            MqttConnectOptions connectOptions = new MqttConnectOptions();

            connectOptions.setCleanSession(cleanSession);

            //Optional username and password
            if(usernameAndPasswordEnabled) {
                System.out.println("setting password");
                connectOptions.setUserName(username);
                connectOptions.setPassword(password.toCharArray());
            }

            //Optional SSL connection
            if(sslEnabled){
                if(caCrtFile.isEmpty()){
                    connectOptions.setSocketFactory( SSLConnection.createSSL(crtFile,keyFile,"") );
                }else{
                    connectOptions.setSocketFactory( SSLConnection.createSSL(caCrtFile,crtFile,keyFile,"") );
                }
            }

            client.setCallback(new OnDeliveryComplete());

            client.connect(connectOptions).waitForCompletion();
            logger.info(this.getName() + " client connected : " + client.isConnected());

            if(client.isConnected()){
                logger.info("MQTTIngestWriter started");
            }else{
                logger.info("MQTTIngestWriter start failed");
                this.setFailed();
                return;
            }

        }catch(Exception e){
            logger.error("", e);
            this.setFailed();
            return;
        }
        ingest(); //does the actual work
    }

    /**
     * This is the stop method which actually does the work on a stop call
     * @return
     */
    @Override
    protected boolean stopImpl() {
        //do whatever to stop doing your async work
        logger.info(String.format("Attempting to stop %s", this.getName()));

        try {
            client.disconnect();
        } catch (MqttException e) {
            logger.error("Failed to disconnect from "+this.getName(), e);
            e.printStackTrace();
        }

        logger.info(String.format("%s stopped", this.getName()));
        return true;
    }

    /**
     * Used for async call back control.
     */
    public final class OnDeliveryComplete implements MqttCallback {

        private OnDeliveryComplete(){
        }

        /**
         * If the connection is lost, attempt to retry until maxRetries are exhausted
         * @param throwable
         */
        @Override
        public void connectionLost(Throwable throwable) {
            logger.warn(getName() + " - Connection lost. " + throwable);

            int retries = 1;
            int maxRetries = 5;
            int timeoutSleep = 5000;

            //Attempt to reconnect client
            try {
                while(!client.isConnected() && retries <= maxRetries){
                    logger.warn(String.format("%s - Attempting to reconnect to %s:%s. Retry %d of %d.\n", getName(), host, port, retries, maxRetries));
                    client.reconnect();
                    retries++;
                    Thread.sleep(timeoutSleep);

                }
            } catch (MqttException | InterruptedException e) {
                logger.error("connectionLost failure", e);
            }

            if(client.isConnected()){
                logger.info(String.format("%s - Reconnect successful.\n", getName()));

                //clientSubscribe();
            }else{
                logger.error(String.format("%s - Reconnect failed.\n", getName()));

                try {
                    client.close();
                } catch (MqttException e) {
                    logger.info("connectionLost: failed closing");
                    //e.printStackTrace();
                }

                setFailed();
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        }
    }

}
