/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.ssl;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SslArtifactReloadTest extends ActiveMQTestBase {

   public static final SimpleString QUEUE = new SimpleString("QueueOverSSL");

   /**
    * This test uses SSL artifacts from {@link org.apache.activemq.artemis.tests.integration.ssl.CoreClientOverOneWaySSLTest}
    */
   private Path tempKeystore;
   private final String PASSWORD = "secureexample";

   private ActiveMQServer server;

   private TransportConfiguration tc;

   @Test
   public void testOneWaySSLReloaded() throws Exception {
      createCustomSslServer();
      server.createQueue(new QueueConfiguration(SslArtifactReloadTest.QUEUE).setRoutingType(RoutingType.ANYCAST).setDurable(false));
      String text = RandomUtil.randomString();

      // create a valid SSL connection and keep it for use later
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator existingLocator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      existingLocator.setCallTimeout(3000);
      ClientSessionFactory existingSessionFactory = addSessionFactory(createSessionFactory(existingLocator));
      ClientSession existingSession = addClientSession(existingSessionFactory.createSession(false, true, true));
      ClientConsumer existingConsumer = addClientConsumer(existingSession.createConsumer(SslArtifactReloadTest.QUEUE));

      // create an invalid SSL connection
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "other-client-side-truststore.jks");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc)).setCallTimeout(3000);
      try {
         addSessionFactory(createSessionFactory(locator));
         fail("Creating session here should fail due to SSL handshake problems.");
      } catch (Exception e) {
         // ignore
      }

      URL otherServerSideKeystore = ClassloadingUtil.findResource("other-server-side-keystore.jks");
      Files.copy(Paths.get(otherServerSideKeystore.toURI()), tempKeystore, StandardCopyOption.REPLACE_EXISTING);

      Thread.sleep(2000);
      // TODO remove sleep(); use tick()

      // create a session with the locator which failed previously proving that the SSL stores have been reloaded
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      ClientProducer producer = addClientProducer(session.createProducer(SslArtifactReloadTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(SslArtifactReloadTest.QUEUE));
      session.start();
      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
      consumer.close();

      // use the existing connection to prove it wasn't lost when the acceptor was reloaded
      existingSession.start();
      m = existingConsumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   private void createCustomSslServer() throws Exception {
      // create a *copy* of the normal server-side keystore so it can be replaced dynamically & safely during the test
      URL serverSideKeystore = ClassloadingUtil.findResource("server-side-keystore.jks");
      tempKeystore = Paths.get(getTemporaryDir() + File.separator + "temp.jks");
      Files.copy(Paths.get(serverSideKeystore.toURI()), tempKeystore);

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, tempKeystore.toString());
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      server = createServer(false, createBasicConfig().setConfigurationFileRefreshPeriod(500).addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "nettySSL")));
      server.start();
      waitForServerToStart(server);
      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
   }
}
