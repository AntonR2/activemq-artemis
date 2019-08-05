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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.QueueAttributes;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

public class RingQueueTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private final SimpleString address = new SimpleString("RingQueueTestAddress");

   private final SimpleString qName = new SimpleString("RingQueueTestQ1");

   @Test
   public void testSimple() throws Exception {
      ServerLocator locator = createNettyNonHALocator().setBlockOnAcknowledge(true).setAckBatchSize(0);
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession clientSession = addClientSession(sf.createSession(false, true, true));
      clientSession.createQueue(address, qName, false, new QueueAttributes().setDurable(true).setRingSize(1L).setMaxConsumers(-1).setPurgeOnNoConsumers(false));
      clientSession.start();
      assertEquals(1, server.locateQueue(qName).getRingSize());

      ClientProducer producer = clientSession.createProducer(address);

      for (int i = 0, j = 0; i < 500; i+=2, j++) {
         ClientMessage m0 = createTextMessage(clientSession, "hello" + i);
         producer.send(m0);
         Wait.assertTrue(() -> server.locateQueue(qName).getMessageCount() == 1, 2000, 100);
         ClientMessage m1 = createTextMessage(clientSession, "hello" + (i + 1));
         producer.send(m1);
         int expectedMessagesReplaced = j + 1;
         Wait.assertTrue(() -> server.locateQueue(qName).getMessagesReplaced() == expectedMessagesReplaced, 2000, 100);
         Wait.assertTrue(() -> server.locateQueue(qName).getMessageCount() == 1, 2000, 100);
         ClientConsumer consumer = clientSession.createConsumer(qName);
         ClientMessage message = consumer.receiveImmediate();
         message.acknowledge();
         consumer.close();
         assertEquals("hello" + (i + 1), message.getBodyBuffer().readString());
      }
   }

   @Test
   public void testRollback() throws Exception {
      ServerLocator locator = createNettyNonHALocator().setBlockOnAcknowledge(true).setAckBatchSize(0);
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession clientSession = addClientSession(sf.createSession(false, true, false));
      clientSession.createQueue(address, qName, false, new QueueAttributes().setDurable(true).setRingSize(1L).setMaxConsumers(-1).setPurgeOnNoConsumers(false));
      clientSession.start();
      assertEquals(1, server.locateQueue(qName).getRingSize());

      ClientProducer producer = clientSession.createProducer(address);

      ClientMessage m0 = createTextMessage(clientSession, "hello0");
      IntegrationTestLogger.LOGGER.info("Sending message");
      producer.send(m0);
      IntegrationTestLogger.LOGGER.info("Sent message");
      Wait.assertTrue(() -> server.locateQueue(qName).getMessageCount() == 1, 2000, 100);

      IntegrationTestLogger.LOGGER.info("Creating consumer");
      ClientConsumer consumer = clientSession.createConsumer(qName);
      IntegrationTestLogger.LOGGER.info("Created consumer");

      IntegrationTestLogger.LOGGER.info("Receiving message");
      ClientMessage message = consumer.receiveImmediate();
      assertNotNull(message);
      IntegrationTestLogger.LOGGER.info("Received message");
      Wait.assertTrue(() -> server.locateQueue(qName).getDeliveringCount() == 1, 2000, 100);

      IntegrationTestLogger.LOGGER.info("Acking message");
      message.acknowledge();
      IntegrationTestLogger.LOGGER.info("Acked message");
      assertEquals("hello0", message.getBodyBuffer().readString());

      ClientMessage m1 = createTextMessage(clientSession, "hello1");
      IntegrationTestLogger.LOGGER.info("Sending message");
      producer.send(m1);
      IntegrationTestLogger.LOGGER.info("Sent message");
      Wait.assertTrue(() -> server.locateQueue(qName).getDeliveringCount() == 2, 2000, 100);
      Wait.assertTrue(() -> server.locateQueue(qName).getMessagesReplaced() == 0, 2000, 100);
      Wait.assertTrue(() -> server.locateQueue(qName).getMessageCount() == 2, 2000, 100);

      IntegrationTestLogger.LOGGER.info("Rolling back message");
      clientSession.rollback();
      IntegrationTestLogger.LOGGER.info("Rolled back message");

      IntegrationTestLogger.LOGGER.info("Closing consumer");
      consumer.close();
      IntegrationTestLogger.LOGGER.info("Closed consumer");
      Wait.assertTrue(() -> server.locateQueue(qName).getDeliveringCount() == 0, 2000, 100);
      Wait.assertTrue(() -> server.locateQueue(qName).getMessagesReplaced() == 1, 2000, 100);
      Wait.assertTrue(() -> server.locateQueue(qName).getMessageCount() == 1, 2000, 100);

      IntegrationTestLogger.LOGGER.info("Creating consumer");
      consumer = clientSession.createConsumer(qName);
      IntegrationTestLogger.LOGGER.info("Created consumer");

      IntegrationTestLogger.LOGGER.info("Receiving message");
      message = consumer.receiveImmediate();
      IntegrationTestLogger.LOGGER.info("Received message");
      assertNotNull(message);
      Wait.assertTrue(() -> server.locateQueue(qName).getDeliveringCount() == 1, 2000, 100);

      IntegrationTestLogger.LOGGER.info("Acking message");
      message.acknowledge();
      IntegrationTestLogger.LOGGER.info("Acked message");

      IntegrationTestLogger.LOGGER.info("Committing session");
      clientSession.commit();
      IntegrationTestLogger.LOGGER.info("Committed session");

      Wait.assertTrue(() -> server.locateQueue(qName).getMessagesAcknowledged() == 1, 2000, 100);
      assertEquals("hello1", message.getBodyBuffer().readString());
   }

   @Test
   public void testScheduled() throws Exception {
      ServerLocator locator = createNettyNonHALocator().setBlockOnAcknowledge(true).setAckBatchSize(0);
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession clientSession = addClientSession(sf.createSession(false, true, false));
      clientSession.createQueue(address, qName, false, new QueueAttributes().setDurable(true).setRingSize(1L).setMaxConsumers(-1).setPurgeOnNoConsumers(false));
      clientSession.start();
      assertEquals(1, server.locateQueue(qName).getRingSize());

      ClientProducer producer = clientSession.createProducer(address);

      ClientMessage m0 = createTextMessage(clientSession, "hello0");
      IntegrationTestLogger.LOGGER.info("Sending message");
      producer.send(m0);
      IntegrationTestLogger.LOGGER.info("Sent message");
      Wait.assertTrue(() -> server.locateQueue(qName).getMessageCount() == 1, 2000, 100);

      // NOT DONE
   }

   @Test
   public void testPaging() throws Exception {

   }

   @Test
   public void testDefault() throws Exception {

   }

   @Test
   public void testDefaultAddressSetting() throws Exception {

   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = addServer(ActiveMQServers.newActiveMQServer(createDefaultNettyConfig(), true));
      // start the server
      server.start();
   }
}
