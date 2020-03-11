/*
 * Copyright (C) 2019  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async.auth;

import static org.jboss.netty.channel.Channels.pipeline;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.xml.bind.DatatypeConverter;

//import org.apache.commons.codec.binary.Base64;
import org.hbase.async.Bytes;
import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.auth.RefreshingSSLContext.RefreshCallback;
import org.hbase.async.auth.RefreshingSSLContext.SourceType;
import org.hbase.async.generated.AuthenticationProtos;
import org.hbase.async.generated.AuthenticationProtos.PropertyEntry;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslContext;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.ssl.SslProvider;
import org.jboss.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.stumbleupon.async.Deferred;

/**
 * 
 * NOTE: Since we're still in Java 8 and don't have a dedicated HTTP client like
 * in JDK9+, AND we don't want to pull in extra JARs, we'll have to hack 
 * together a client with the Netty library. We'll use the HttpSnoop example from
 * Netty and make sure to set our SSL context in client mode.
 *
 */
public class MTLSClientAuthProvider extends ClientAuthProvider
    implements RefreshCallback {
  private static Logger LOG = LoggerFactory.getLogger(MTLSClientAuthProvider.class);
  
  private static final String TOKEN_RENEWAL_KEY = "hbase.client.mtls.token.renewalPeriod";
  private static final String CERT_PATH_KEY = "hbase.client.mtls.certificate";
  private static final String CERT_KEY_KEY = "hbase.client.mtls.key";
  private static final String CERT_CA_KEY = "hbase.client.mtls.ca";
  private static final String CERT_KEYSTORE_KEY = "hbase.client.mtls.keystore";
  private static final String CERT_KEYSTORE_PASS_KEY = "hbase.client.mtls.keystore.pass";
  private static final String RETRY_INTERVAL_KEY = "hbase.client.mtls.retry.interval";
  private static final String TOKEN_REGISTRY_KEY = "hbase.client.mtls.token.registry";
  
  private static final Pattern TOKEN_REGEX = Pattern.compile("\\{\"token\":\"(.*)\"\\}");

  public static final String SASL_DEFAULT_REALM = "default";
  private static final String SERVICE = "HBASE_AUTH_TOKEN";
  private static final String[] MECHANISM = { "DIGEST-MD5" };
  
  private static final Map<String, String> BASIC_JAAS_OPTIONS =
      new HashMap<String,String>();
  static {
    String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
    if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
      BASIC_JAAS_OPTIONS.put("debug", "true");
    }
  }
  
  private static final AppConfigurationEntry MYLOGIN =
    new AppConfigurationEntry(HadoopLoginModule.class.getName(),
                              LoginModuleControlFlag.REQUIRED,
                              BASIC_JAAS_OPTIONS);
  private static class DynamicConfiguration
    extends javax.security.auth.login.Configuration {
    private AppConfigurationEntry[] ace;
    
    DynamicConfiguration(AppConfigurationEntry[] ace) {
      this.ace = ace;
    }
    
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      return ace;
    }
  }
  
  private final RefreshingSSLContext context;
  private final String hostname;
  private volatile boolean initial_run;
  private volatile Throwable token_exception;
  private volatile String token_response;
  private volatile int token_status;
  private int last_attempt;
  private volatile AuthenticationProtos.TokenIdentifier identifier;
  private volatile Subject subject;
  private volatile LoginContext login_context;
  
  private volatile byte[] id;
  private volatile byte[] pass;
  
  public MTLSClientAuthProvider(final HBaseClient hbase_client) {
    super(hbase_client);
    LOG.info("Initializing MTLS client auth.");
    initial_run = true;
    if (Strings.isNullOrEmpty(hbase_client.getConfig().getString(CERT_KEYSTORE_KEY))) {
      context = RefreshingSSLContext.newBuilder()
          .setCert(hbase_client.getConfig().getString(CERT_PATH_KEY))
          .setCa(hbase_client.getConfig().getString(CERT_CA_KEY))
          .setKey(hbase_client.getConfig().getString(CERT_KEY_KEY))
          .setInterval(hbase_client.getConfig().getInt(TOKEN_RENEWAL_KEY))
          .setTimer(hbase_client.getTimer())
          .setType(SourceType.FILES)
          .setCallback(this)
          .build();
    } else {
      context = RefreshingSSLContext.newBuilder()
          .setKeystore(hbase_client.getConfig().getString(CERT_KEYSTORE_KEY))
          .setKeystorePass(hbase_client.getConfig().getString(CERT_KEYSTORE_PASS_KEY))
          .setInterval(hbase_client.getConfig().getInt(TOKEN_RENEWAL_KEY))
          .setTimer(hbase_client.getTimer())
          .setType(SourceType.KEYSTORE)
          .setCallback(this)
          .build();
    }
    
    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Can't get hostname", e);
    }
    try {
      fetchToken().join(30000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    // VALIDATE and RETRIES
  }
  
  // TEMP
  public MTLSClientAuthProvider(RefreshingSSLContext context) {
    super(null);
    initial_run = true;
    this.context = context;
    
    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Can't get hostname", e);
    }
    
    System.out.println("FETCHING.........");
    fetchToken();
    System.out.println("DONE FETCHING!");
    // VALIDATE and RETRIES
  }

  static final class SimplePrinciple implements Principal {
    final String user;
    
    SimplePrinciple(final String user) {
      this.user = user;
    }
    
    @Override
    public String getName() {
      return user;
    }
    
  }
  
  @Override
  public SaslClient newSaslClient(String service_ip,
      Map<String, String> props) {
    try {
      //props.put("com.sun.security.sasl.digest.cipher", "md5-sess");
      final Login client_login = Login.getCurrentLogin();
      
      
      class SaslClientCallbackHandler implements CallbackHandler {
//        private final String userName;
//        private final char[] userPassword;

//        public SaslClientCallbackHandler() {
//          this.userName = new String(identifier.getUsername().toByteArray());//SaslUtil.encodeIdentifier(token.getIdentifier());
//          this.userPassword = new char[0];//SaslUtil.encodePassword(token.getPassword());
//        }

        public void handle(Callback[] callbacks)
            throws UnsupportedCallbackException {
          System.out.println("              HANDLING SASL CALLBACK!");
          NameCallback nc = null;
          PasswordCallback pc = null;
          RealmCallback rc = null;
          for (Callback callback : callbacks) {
            System.out.println("     CALLBACK: " + callback.getClass());
            if (callback instanceof RealmChoiceCallback) {
              continue;
            } else if (callback instanceof NameCallback) {
              nc = (NameCallback) callback;
            } else if (callback instanceof PasswordCallback) {
              pc = (PasswordCallback) callback;
            } else if (callback instanceof RealmCallback) {
              rc = (RealmCallback) callback;
            } else {
              throw new UnsupportedCallbackException(callback,
                  "Unrecognized SASL client callback");
            }
          }
          if (nc != null) {
            if (LOG.isDebugEnabled())
              LOG.debug("SASL client callback: setting username: " + new String(id));
            nc.setName(Base64.getEncoder().encodeToString(id));
          }
          if (pc != null) {
            if (LOG.isDebugEnabled())
              LOG.debug("SASL client callback: setting userPassword");
            pc.setPassword(Base64.getEncoder().encodeToString(pass).toCharArray());
          }
          if (rc != null) {
            if (LOG.isDebugEnabled())
              LOG.debug("SASL client callback: setting realm: "
                  + rc.getDefaultText());
            rc.setText(rc.getDefaultText());
          }
          
          
        }
      }
      final SaslClientCallbackHandler hndlr = new SaslClientCallbackHandler();
      final class PriviledgedAction implements 
          PrivilegedExceptionAction<SaslClient> {
        @Override
        public SaslClient run() throws Exception {
          LOG.info("Client will use " + MECHANISM[0] + " as SASL mechanism with MTLS Temp.");
//          LOG.debug("Creating sasl client: client=" + 
//              new String(identifier.getUsername().toByteArray()) +
//              ", service=" + SERVICE + ", serviceHostname=" + hostname);
          
          System.out.println("    PROPS: " + props);
          return Sasl.createSaslClient(
              MECHANISM, 
              null,              // authorization ID
              null,
              "default", 
              props, 
              hndlr);             // callback
        }
        @Override
        public String toString() {
          return "create sasl client";
        }
      }
      DynamicConfiguration conf = new DynamicConfiguration(new AppConfigurationEntry[] { MYLOGIN });
      //login_context = new LoginContext(HadoopLoginModule.class.getName(), hndlr);
      
      Principal p = new SimplePrinciple("ymsgrid"); //new String(identifier.getUsername().toByteArray())); 
      AccessControlContext context = AccessController.getContext();
      Subject s = Subject.getSubject(context);
      System.out.println("  CURRENT SUBJECT: " + s);
      Subject sub = new Subject(false,
          Sets.newHashSet(p), Sets.newHashSet(identifier), Sets.newHashSet());
      login_context = new LoginContext("digest", sub, hndlr, conf);
      login_context.login();
      
      
      subject = sub;
      //System.out.println("  NEW SUBJECT: " + subject);
      return Subject.doAs(sub, new PriviledgedAction());
    } catch (Exception e) {
      LOG.error("Error creating SASL client", e);
      throw new IllegalStateException("Error creating SASL client", e);
    }
  }

  @Override
  public String getClientUsername() {
    //try {
      return "ygrid";//identifier.getUsername().toString("UTF-8");
//    } catch (UnsupportedEncodingException e) {
//      throw new RuntimeException("WTF?", e);
//    }
  }

  @Override
  public byte getAuthMethodCode() {
    return ClientAuthProvider.DIGEST_CLIENT_AUTH_CODE;
  }

  @Override
  public Subject getClientSubject() {
    return subject;
  }
  
  @Override
  public void refresh(final SSLContext context) {
    synchronized (this) {
      if (initial_run) {
        initial_run = false;
        return;
      }
    }
    
    try {
      fetchToken().join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  Deferred<Boolean> fetchToken() {
    if ((last_attempt > 0 && 
        System.currentTimeMillis() / 1000 - last_attempt < 
            hbase_client.getConfig().getInt(RETRY_INTERVAL_KEY)) && token_exception != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting " + (System.currentTimeMillis() / 1000 - last_attempt) 
          + " seconds until we try fetching another token.");
      }
      return Deferred.fromResult(false);
    }
    last_attempt = (int) System.currentTimeMillis() / 1000;
    
    Deferred<Boolean> deferred = new Deferred<Boolean>();
    final URI uri = URI.create(hbase_client.getConfig().getString(TOKEN_REGISTRY_KEY));
    //final URI uri = URI.create("https://reluxblue-hb-pxy.blue.ygrid.yahoo.com:4443/v1/.system/delegationToken");
    final String host = uri.getHost() == null? "127.0.0.1" : uri.getHost();
    int port = uri.getPort();
    if (port < 1) {
      port = 443;
    }
    
    final ClientBootstrap bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
    //try {
      bootstrap.setPipelineFactory(new HttpClientPipelineFactory(host, port));
      System.out.println("Connecting to host: " + host + ":" + port);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to host: " + host + ":" + port);
      }
      final ChannelFuture future = bootstrap.connect(
          new InetSocketAddress(host, port));
      
        future.addListener(new ChannelFutureListener() {
          
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            LOG.info("In the channel callback!");
         // Prepare the HTTP request.
            final HttpRequest request = new DefaultHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath());
            request.headers().set(HttpHeaders.Names.HOST, host);
            request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
            request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
            
            System.out.println("Requesting: " + uri);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Requesting: " + uri);
            }
            // Send the HTTP request.
            future.getChannel().write(request);

            
            future.getChannel().getCloseFuture().addListener(new ChannelFutureListener() {

              @Override
              public void operationComplete(ChannelFuture future) throws Exception {
                System.out.println("IN OPERATION COMPLETE");
                // TODO Auto-generated method stub
                // Wait for the server to close the connection.
                //channel.getCloseFuture().sync();
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Connection to token server closed.");
                }
                
                // now we can process the response
                if (token_status == 200) {
                  final Matcher matcher = TOKEN_REGEX.matcher(token_response);
                  if (matcher.matches()) {
                    token_response = matcher.group(1);
                    parseToken(token_response);
                    deferred.callback(true);
                  } else {
                    token_exception = new RuntimeException("Failed to match token.");
                    deferred.callback(token_exception);
                  }
                }
                
                class TO implements TimerTask {

                  @Override
                  public void run(Timeout timeout) throws Exception {                    
                    bootstrap.releaseExternalResources();
                  }
                  
                }
                hbase_client.getTimer().newTimeout(new TO(), 1, TimeUnit.MILLISECONDS);
              }
              
            });
          }
          
        });
      
      // Wait until the connection attempt succeeds or fails.
      //final Channel channel = future.sync().getChannel();
      
      
//      
//    } catch (InterruptedException e) {
//      LOG.error("Interrupted during request to token server.", e);
//    }
//    } finally {
//      // Shut down executor threads to finish.
//      bootstrap.releaseExternalResources();
//    }
    return deferred;
  }
  
  public static void main(String[] args) {
    System.out.println("      ARGS: " + args);
    if (args != null) 
      System.out.println("          " + Arrays.toString(args));
    if (true) {
      HBaseClient client = null;
      
      boolean kerb = args.length > 0;
      System.out.println("------------ KERB? " + kerb);
      String f = kerb ? "/home/clarsen/hbase2.conf" : "/home/clarsen/hbase.conf";
      System.out.println("      Conf: " + f);
      try {
        Config config = new Config(f);
        client = new HBaseClient(config);
        GetRequest get = new GetRequest("yamas:tsdb".getBytes(), new byte[] { 0 });
        
        ArrayList<KeyValue> row = client.get(get).join();
        System.out.println(row);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } finally {
        if (client != null) {
          try {
            client.shutdown().join();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
      
      
      //String t = "IwAAAB8IABIHeW1zZ3JpZBjTDyDEmf3e-i0oxKGv__wtMJ1rFEzi6LlCst0VURf8iLLulPXLMM3vEEhCQVNFX0FVVEhfVE9LRU4kMWM1ZWZkNDktMTE2OC00ZDQzLWFiNGYtYjBiNzg3MDBhOWM1";
//      String t = "IwAAAB8IABIHeW1zZ3JpZBjTDyDXq8De-i0o17Py_vwtMJNrFE8I7lWlnDyrs08yJVhWrCRIj9uLEEhCQVNFX0FVVEhfVE9LRU4kMWM1ZWZkNDktMTE2OC00ZDQzLWFiNGYtYjBiNzg3MDBhOWM1";
//      parseToken(t);
//      System.exit(0);
    }
    
    
    Timer t = new HashedWheelTimer();
    URI uri = null;
    try {
      uri = new URI("https://reluxblue-hb-pxy.blue.ygrid.yahoo.com:4443/v1/.system/delegationToken");
      //uri = new URI("https://tsdbconfig.yamas.ouroath.com/prod/middletier_cutover.yaml");
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    String scheme = uri.getScheme() == null? "http" : uri.getScheme();
    String host = uri.getHost() == null? "127.0.0.1" : uri.getHost();
    int port = uri.getPort();
    if (port == -1) {
        if ("http".equalsIgnoreCase(scheme)) {
            port = 80;
        } else if ("https".equalsIgnoreCase(scheme)) {
            port = 443;
        }
    }
    System.out.println("PORT: " + port);

    if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
        System.err.println("Only HTTP(S) is supported.");
        return;
    }
    RefreshingSSLContext context = null;
    try {
      
    //zts-rolecert -svc-key-file /Users/clarsen/.athenz/key -svc-cert-file /Users/clarsen/.athenz/cert -role-domain griduser -role-name uid.ymsgrid -role-cert-file grid.ymsgrid.cert -dns-domain zts.yahoo.cloud -zts https://zts.athens.yahoo.com:4443/zts/v1
    context = RefreshingSSLContext.newBuilder()
        .setCert("/Users/clarsen/Documents/opentsdb/temp/grid.ymsgrid.cert")
        .setCa("/Users/clarsen/Documents/opentsdb/temp/yahoo_certificate_bundle.pem")
        .setKey("/Users/clarsen/.athenz/key")
        .setInterval(300000)
        .setType(SourceType.FILES)
        .setTimer(t)
        .build();
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("    CTX: " + context.context());
    
    MTLSClientAuthProvider mtls = new MTLSClientAuthProvider(context);
    
    if (mtls.token_exception != null) {
      mtls.token_exception.printStackTrace();
    } else {
      System.out.println("RESPONSE: " + mtls.token_status +"\n" + mtls.token_response);
    }
    
    // Configure the client.
//    ClientBootstrap bootstrap = new ClientBootstrap(
//            new NioClientSocketChannelFactory(
//                    Executors.newCachedThreadPool(),
//                    Executors.newCachedThreadPool()));
//
//    try {
//        // Set up the event pipeline factory.
//        bootstrap.setPipelineFactory(new HttpClientPipelineFactory(host, port));
//
//        // Start the connection attempt.
//        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
//        System.out.println("Connected.....");
//        // Wait until the connection attempt succeeds or fails.
//        Channel channel = future.sync().getChannel();
//        System.out.println("Got channel....");
//        // Prepare the HTTP request.
//        HttpRequest request = new DefaultHttpRequest(
//                HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
//        request.headers().set(HttpHeaders.Names.HOST, host);
//        request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
//        request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
//        
//        // Send the HTTP request.
//        channel.write(request);
//
//        // Wait for the server to close the connection.
//        channel.getCloseFuture().sync();
//    } catch (InterruptedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    } finally {
//        // Shut down executor threads to exit.
//        bootstrap.releaseExternalResources();
//        t.stop();
//    }
    System.out.println("..... exiting.");
  }
  
  class HttpClientPipelineFactory implements ChannelPipelineFactory {
    
    private final String host;
    private final int port;

    public HttpClientPipelineFactory(final String host, final int port) {
      this.host = host;
      this.port = port;
    }

    public ChannelPipeline getPipeline() {
      // Create a default pipeline implementation.
      final ChannelPipeline pipeline = pipeline();
      final SSLEngine engine = context.context().createSSLEngine(host, port);
      engine.setUseClientMode(true);
      SslHandler hndlr = new SslHandler(engine);
      pipeline.addLast("ssl", hndlr);

      pipeline.addLast("codec", new HttpClientCodec());
      // Remove the following line if you don't want automatic content decompression.
      pipeline.addLast("inflater", new HttpContentDecompressor());

      pipeline.addLast("handler", new HttpHandler());
      return pipeline;
    }
  }
  
  class HttpHandler extends SimpleChannelUpstreamHandler {

    private boolean readingChunks;
    private StringBuilder buffer;
    
    HttpHandler() {
      buffer = new StringBuilder();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, 
                                final ExceptionEvent e) throws Exception {
      LOG.error("Failed call", e.getCause());
      token_exception = e.getCause();
      ctx.getChannel().close();
    }
    
    @Override
    public void messageReceived(final ChannelHandlerContext ctx, 
                                final MessageEvent e) {
      LOG.debug("MSG RECEIVED!");
      if (!readingChunks) {
        final HttpResponse response = (HttpResponse) e.getMessage();
        token_status = response.getStatus().getCode();
        
        if (response.isChunked()) {
          readingChunks = true;
        } else {
          final ChannelBuffer content = response.getContent();
          if (content.readable()) {
            buffer.append(content.toString(CharsetUtil.UTF_8));
          }
          token_response = buffer.toString();
          buffer = null;
        }
      } else {
        final HttpChunk chunk = (HttpChunk) e.getMessage();
        if (chunk.isLast()) {
          readingChunks = false;
          token_response = buffer.toString();
          // TODO - verify if this last chunk is always empty.
        } else {
          buffer.append(chunk.getContent().toString(CharsetUtil.UTF_8));
        }
      }
    }
  }

  
  void parseToken(final String token) {
//    Base64 decoder = new Base64(0, null, true);
//    byte[] data = decoder.decode(token);
    byte[] data = Base64.getUrlDecoder().decode(token);
    System.out.println("     B64 DECODE: " + Arrays.toString(data));
    System.out.println("     B64 LENGTH: " + data.length);
    System.out.println("     B64 String: " + new String(data));
    
//    int i = 5;
//    byte[] temp = Arrays.copyOfRange(data, i, data.length);
//    System.out.println(Arrays.toString(temp));
//    System.out.println(new String(temp));
    try {
    
    int[] offset = new int[] { 0 };
    int len = MTLSClientAuthProvider.readVInt(data, offset);
    System.out.println("LEN: " + len);
    id = Arrays.copyOfRange(data, offset[0], offset[0] + len);
    offset[0] += len;

    // PASSWord
    len = MTLSClientAuthProvider.readVInt(data, offset);
    System.out.println("LEN: " + len);
    pass = Arrays.copyOfRange(data, offset[0], offset[0] + len);
    System.out.println("PASS  " + Arrays.toString(data) + "  AS STRING: " + new String(data));

    
    // TOKEN: https://git.ouroath.com/hadoop/Hadoop/blob/y-branch-2.8/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/Token.java#L327-L334
    
    // https://git.ouroath.com/hadoop/hbase/blob/y1.3/hbase-client/src/main/java/org/apache/hadoop/hbase/protobuf/ProtobufUtil.java#L3344
    CodedInputStream cis = CodedInputStream.newInstance(data);
//    
    System.out.println("TEMP: " + data.length);

//      AuthenticationProtos.Token identifier = 
//          AuthenticationProtos.Token.newBuilder().mergeFrom(temp).build();
//      identifier =
//          AuthenticationProtos.TokenIdentifier.newBuilder().mergeFrom(cis).build();
////      AuthenticationProtos.TokenIdentifier identifier =
////          AuthenticationProtos.TokenIdentifier.parseFrom(temp);
//      System.out.println("User: " + new String(identifier.getUsername().toByteArray()));
//      System.out.println("Kind: " + identifier.getKind());
//      System.out.println("Key ID: " + identifier.getKeyId());
//      System.out.println("Issue: " + identifier.getIssueDate());
//      System.out.println("Expir: " + identifier.getExpirationDate());
//      System.out.println("Seq: " + identifier.getSequenceNumber());
//      System.out.println("Props: " + identifier.getPropertiesCount());
//      for (PropertyEntry pe : identifier.getPropertiesList()) {
//        System.out.println(pe.getKey() + "  " + pe.getValue());
//      }
//      System.out.println("Serialized size: " + identifier.getSerializedSize());
//
//      i += identifier.getSerializedSize() - 1;
      
      
    } catch (InvalidProtocolBufferException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static class HadoopLoginModule implements LoginModule {
    private Subject subject;

    @Override
    public boolean abort() throws LoginException {
      System.out.println("    [HLM] ABORT!");
      return true;
    }

    private <T extends Principal> T getCanonicalUser(Class<T> cls) {
      System.out.println("    [HLM] GET CANNONICAL USER!");
      for(T user: subject.getPrincipals(cls)) {
        return user;
      }
      return null;
    }

    private static Class<? extends Principal>[] principalTypes = new Class[]{
//      User.class,
//      X500Principal.class,
//      KerberosPrincipal.class,
//      OS_PRINCIPAL_CLASS,
        SimplePrinciple.class
    };

    private Principal getBestCanonicalUser() {
      System.out.println("    [HLM] GET BEST CANNONICAL USER!");
      Principal user = null;
      for (Class<? extends Principal> principalType : principalTypes) {
        user = getCanonicalUser(principalType);
        if (user != null) {
          break;
        }
      }
      return user;
    }

    @Override
    public boolean commit() throws LoginException {
      System.out.println("    [HLM] COMMIT!");
      if (LOG.isDebugEnabled()) {
        LOG.debug("hadoop login commit");
      }
      Principal user = getBestCanonicalUser();
      // if we already have a user, we are done.
      if (user instanceof SimplePrinciple) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("using existing subject:"+subject.getPrincipals());
        }
        return true;
      }
      //If we don't have a secure user and security is disabled, check
      //if user is specified in the environment or properties
//      if (!isSecurityEnabled() && (user == null ||
//          user.getClass() == OS_PRINCIPAL_CLASS)) {
//        String envUser = System.getenv(HADOOP_USER_NAME);
//        if (envUser == null) {
//          envUser = System.getProperty(HADOOP_USER_NAME);
//        }
//        if (envUser != null) {
//          user = new User(envUser);
//        }
//      }
      // if we found the user, add our principal
      if (user != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using user: \"" + user + "\" with name " + user.getName());
        }

//        User userEntry = null;
//        try {
//          userEntry = User.fromPrincipal(user);
//        } catch (Exception e) {
//          throw (LoginException)(new LoginException(e.toString()).initCause(e));
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug("User entry: \"" + userEntry.toString() + "\"" );
//        }
//
//        subject.getPrincipals().add(userEntry);
        return true;
      }
      LOG.error("Can't find user in " + subject);
      throw new LoginException("Can't find user name");
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
      System.out.println("    [HLM] INITIALIZE!");
      this.subject = subject;
    }

    @Override
    public boolean login() throws LoginException {
      System.out.println("    [HLM] LOGIN!");
      if (LOG.isDebugEnabled()) {
        LOG.debug("hadoop login");
      }
      return true;
    }

    @Override
    public boolean logout() throws LoginException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("hadoop logout");
      }
      return true;
    }
  }

  public static int readVInt(byte[] stream, int[] offset) throws IOException {
    long n = readVLong(stream, offset);
    if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
      throw new IOException("value too long to fit in integer");
    }
    return (int)n;
  }
  
  public static long readVLong(byte[] stream, int[] offset) throws IOException {
    byte firstByte = stream[offset[0]++];
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = stream[offset[0]++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }
  
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }
  
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }
}
