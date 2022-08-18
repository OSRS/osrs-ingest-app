/**
 * ***********************************************************************
 * Copyright (c) 2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ***********************************************************************
 */
package optix.time.ingest.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

import javax.net.ssl.*;
import java.io.FileReader;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

/**
 * Utility class that provides methods to manage SSL connections.
 */
public final class SSLConnection {

    private SSLConnection(){}

    private static Object syncRoot=new Object();
    private static boolean inited=false;
    public static void init(){
        synchronized (syncRoot) {
            if (inited)
                return;

            /**
             * Add BouncyCastle as a Security Provider
             */
            Security.addProvider(new BouncyCastleProvider());
            inited=true;
        }
    }

    final static Log logger = LogFactory.getLog(SSLConnection.class);

    /**
     * Establish a SSLSocketFactory with no CA validation
     * @param caCrtFile - Absolute path to x509 Certificate Authority (CA) file
     * @param crtFile - Absolute path to x509 certificate file
     * @param keyFile - Absolute path to RSA pem file
     * @param password - Password to decrypt keyFile. Pass in "" if no password is required
     * @return - SSLSocketFactory initialized with the provided parameters
     */
    public static SSLSocketFactory createSSL(final String caCrtFile, final String crtFile, final String keyFile,
                                             final String password) {
        if (caCrtFile!=null)
            return createSSL(new String[]{caCrtFile}, crtFile, keyFile, password);
        return createSSL((String[])null, crtFile, keyFile, password);
    }

    /**
     * Establish a SSLSocketFactory with no CA validation
     * @param crtFile - Absolute path to x509 certificate file
     * @param keyFile - Absolute path to RSA pem file
     * @param password - Password to decrypt keyFile. Pass in "" if no password is required
     * @return - SSLSocketFactory initialized with the provided parameters with no CA validation
     */
    public static SSLSocketFactory createSSL(final String crtFile, final String keyFile,
                                             final String password) {
        return createSSL((String[])null, crtFile, keyFile, password);
    }

    public static SSLSocketFactory createSSL(final String[] caCrtFiles, final String crtFile, final String keyFile, final String password){
        try {
            if (crtFile!=null && keyFile!=null && crtFile.length()>0 && keyFile.length()>0) {
                init(); //safe to call multiple times
                JcaX509CertificateConverter certificateConverter = new JcaX509CertificateConverter().setProvider("BC");

                TrustManager[] caTrusts = createCATrust(caCrtFiles, certificateConverter);

                /**
                 * Load client certificate
                 */
                PEMParser reader = new PEMParser(new FileReader(crtFile));
                X509CertificateHolder certHolder = (X509CertificateHolder) reader.readObject();
                reader.close();

                X509Certificate cert = certificateConverter.getCertificate(certHolder);

                /**
                 * Load client private key
                 */
                reader = new PEMParser(new FileReader(keyFile));
                Object keyObject = reader.readObject();
                reader.close();

                PEMDecryptorProvider provider;
                if (password!=null && password.length()>0){
                    provider=new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
                } else{
                    provider=new JcePEMDecryptorProviderBuilder().build(new char[]{});
                }
                JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter().setProvider("BC");

                KeyPair key;

                if (keyObject instanceof PEMEncryptedKeyPair) {
                    key = keyConverter.getKeyPair(((PEMEncryptedKeyPair) keyObject).decryptKeyPair(provider));
                } else {
                    key = keyConverter.getKeyPair((PEMKeyPair) keyObject);
                }

                /**
                 * Client key and certificates are sent to server so it can authenticate the client
                 */
                KeyStore clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                clientKeyStore.load(null, null);
                clientKeyStore.setCertificateEntry("certificate", cert);
                clientKeyStore.setKeyEntry("private-key", key.getPrivate(), password.toCharArray(),
                        new Certificate[]{cert});

                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                        KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(clientKeyStore, password.toCharArray());

                /**
                 * Create SSL socket factory
                 */
                SSLContext context = SSLContext.getInstance("TLSv1.2");
                context.init(keyManagerFactory.getKeyManagers(), caTrusts, null);

                /**
                 * Return the newly created socket factory object
                 */
                return context.getSocketFactory();
            }

        } catch (Exception e) {
            logger.error("", e);
        }

        return null;
    }


    private static TrustManager[] createCATrust(String[] caCrtFiles, JcaX509CertificateConverter certificateConverter){
        try {
            /**
             * CA certificate is used to authenticate server
             */
            KeyStore caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            caKeyStore.load(null, null);

            /**
             * Load Certificate Authority (CA) certificate
             */
            if (caCrtFiles != null && caCrtFiles.length > 0) {
                if (caCrtFiles.length == 1) {
                    PEMParser reader = new PEMParser(new FileReader(caCrtFiles[0]));
                    X509CertificateHolder caCertHolder = (X509CertificateHolder) reader.readObject();
                    reader.close();

                    X509Certificate caCert = certificateConverter.getCertificate(caCertHolder);
                    caKeyStore.setCertificateEntry("ca-certificate", caCert);
                } else {
                    for (int i=0;i<caCrtFiles.length;i++) {
                        PEMParser reader = new PEMParser(new FileReader(caCrtFiles[i]));
                        X509CertificateHolder caCertHolder = (X509CertificateHolder) reader.readObject();
                        reader.close();

                        X509Certificate caCert = certificateConverter.getCertificate(caCertHolder);
                        caKeyStore.setCertificateEntry("ca-certificate-"+i, caCert);
                    }
                }

                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                        TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(caKeyStore);
                return trustManagerFactory.getTrustManagers();
            } else
                return createCATrust();
        }catch(Exception e){}

        return null;
    }

    private static TrustManager[] createCATrust(){
        /**
         * Create SSL socket factory
         */
        TrustManager[] trustAllCerts = new TrustManager [] {new X509ExtendedTrustManager () {
            @Override
            public void checkClientTrusted (X509Certificate [] chain, String authType, Socket socket) {

            }

            @Override
            public void checkServerTrusted (X509Certificate [] chain, String authType, Socket socket) {

            }

            @Override
            public void checkClientTrusted (X509Certificate [] chain, String authType, SSLEngine engine) {

            }

            @Override
            public void checkServerTrusted (X509Certificate [] chain, String authType, SSLEngine engine) {

            }

            @Override
            public java.security.cert.X509Certificate [] getAcceptedIssuers () {
                return null;
            }

            @Override
            public void checkClientTrusted (X509Certificate [] certs, String authType) {
            }

            @Override
            public void checkServerTrusted (X509Certificate [] certs, String authType) {
            }

        }};

        return trustAllCerts;
    }
}
