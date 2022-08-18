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

import optix.time.ingest.IngestConfiguration;
import optix.time.ingest.IngestEngine;
import optix.time.ingest.utils.SSLConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * IngestSource which connects to an MQTT broker synchronously and receives messages asynchronously. Connection supports basic tcp, or ssl with the following config options set:
 *   "sslEnabled": "true",
 *   "caCrtFile" : "/path/to/my.ca",
 *   "crtFile": "/path/to/my.cer",
 *   "keyFile": "/path/to/my.key"
 *
 * Additionally, a username and password can be set in the configuration to use with either tcp or ssl
 *   "username": "myuser",
 *   "password": "mypassword",
 *
 * All messages received will be written in binary and will flow through the GIA on the binary pipeline.
 *
 * MQTT Example code taken from
 * https://www.programcreek.com/java-api-examples/?code=trustedanalytics%2Fmqtt-listener-example%2Fmqtt-listener-example-master%2Fsrc%2Fmain%2Fjava%2Forg%2Ftrustedanalytics%2Fmqttlistener%2Fingestion%2FIngestion.java#
 *
 * Async example, however the default MQTTClient is sufficient for this example as messages received are still handled async.
 * https://stackoverflow.com/questions/46580780/how-to-solve-async-connection-issue-of-paho-mqtt-client
 */
public final class MQTTIngestSource extends AbstractIngestSource {

    final static Log logger = LogFactory.getLog(MQTTIngestSource.class);

    private String host = "";
    private String port = "";

    //Default values of optional config parameters
    private int qualityOfService = 0;
    private String subscribeTopic = "#";
    private boolean cleanSession = true;
    private boolean sslEnabled = false;
    private boolean usernameAndPasswordEnabled = false;

    //Optional authentication parameters
    private String caCrtFile = "";
    private String crtFile = "";
    private String keyFile = "";
    private String username = "";
    private String password = "";

    private MqttClient client;

    /**
     * This is the initialization method which actually does the work.
     * @return
     */
    @Override
    protected boolean initImpl() {
        logger.info("Initializing MQTTIngestSource");
        IngestConfiguration.SourceConfiguration cfg = IngestConfiguration.getInstance().getSource(this.getName());
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

                if(cfg.containsKey("topic")){
                    subscribeTopic = cfg.get("topic");
                }

                //Authentication mechanisms
                if(cfg.containsKey("sslEnabled")){
                    sslEnabled = Boolean.valueOf(cfg.get("sslEnabled"));

                    if(sslEnabled){

                        //caCrtFile is optional. If not provided no CA validation will be performed when establishing the SSL connection.
                        if(cfg.containsKey("caCrtFile")){
                            caCrtFile = cfg.get("caCrtFile");
                        }

                        //The following are now required parameters since sslEnabled is true
                        if(cfg.containsKey("crtFile")){
                            crtFile = cfg.get("crtFile");
                        }else{
                            logger.error(this.getName() + " does not contain 'crtFile' in config. Required when 'sslEnabled' is true");
                            return false;
                        }

                        if(cfg.containsKey("keyFile")){
                            keyFile = cfg.get("keyFile");
                        }else{
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

                logger.info("MQTTIngestSource initialized");
                return true;
            }

        }
        return false;
    }

    /**
     * Connect to the MQTT broker and start the ingest WorkPool
     */
    @Override
    protected void run() {
        logger.info(String.format("Attempting to start %s", this.getName()));

        //Attempt to connect to MQTT Broker
        try{

            String connectType = "tcp";
            if(sslEnabled){
                connectType = "ssl";
            }

            String connectionURI = String.format("%s://%s:%s", connectType, host, port);

            client = new MqttClient(connectionURI, IngestConfiguration.getInstance().getName(), new MemoryPersistence());
            MqttConnectOptions connectOptions = new MqttConnectOptions();

            //False to allow broker to queue messages for this client should the connection be lost
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

            client.setCallback(new OnMessageArrived());
            client.connect(connectOptions);
            logger.info(this.getName() + " client connected : " + client.isConnected());
            clientSubscribe();

            if(client.isConnected()){
                logger.info(String.format("%s started", this.getName()));
            }else{
                this.setFailed();
                logger.info(String.format("%s failed to start", this.getName()));
            }
        }catch(Exception e){
            logger.error(this.getName() + " had an unknown error", e);
            this.setFailed();
        }
    }

    //Subscribe to a topic on the broker
    private void clientSubscribe(){
        if(client.isConnected()){
            try {
                client.subscribe(subscribeTopic, qualityOfService);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
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
            logger.error("", e);
            e.printStackTrace();
        }

        logger.info(String.format("%s stopped", this.getName()));
        return true;
    }

    //The topic we send to the router is the specific message's topic we receive from the broker
    private void write(String topic, String msg){
        logger.debug(String.format("%s - Writing data to router", this.getName()));
        IngestEngine.getInstance().getRouter().write(this.getName(), topic, msg);
    }

    private void write(String topic, byte[] msg){
        logger.debug(String.format("%s - Writing data to router", this.getName()));
        IngestEngine.getInstance().getRouter().writeBinary(this.getName(), topic, msg);
    }

    /**
     * Used for async call back control.
     */
    public final class OnMessageArrived implements MqttCallback {

        private OnMessageArrived(){ }

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
                e.printStackTrace();
            }

            if(client.isConnected()){
                logger.info(String.format("%s - Reconnect successful.\n", getName()));

                clientSubscribe();
            }else{
                logger.error(String.format("%s - Reconnect failed.\n", getName()));

                try {
                    client.close();
                } catch (MqttException e) {
                    e.printStackTrace();
                }

                setFailed();
            }
        }

        /**
         * Write a received message as binary
         * @param topic
         * @param mqttMessage
         * @throws Exception
         */
        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
            try {
                logger.debug(String.format("%s\ntopic: %s\nmessage: %s", getName(), topic, mqttMessage));
            }catch (Exception e){}

            write(topic, mqttMessage.getPayload());
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        }
    }


}


