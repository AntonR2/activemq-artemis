/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.server;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

/**
 * A simple test-case used for documentation purposes.
 */
public class RetroactiveAddressTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   protected ClientSession session;

   protected ClientSessionFactory sf;

   protected ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, createDefaultInVMConfig());
      server.getConfiguration().setThreadPoolMaxSize(200);
      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   @Test
   public void testRetroactiveResourceCreation() throws Exception {
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertAddress = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.ADDRESS);
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      final SimpleString divert = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.DIVERT);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(10));
      server.addAddressInfo(new AddressInfo(addressName));
      assertNotNull(server.getAddressInfo(divertAddress));
      assertNotNull(server.locateQueue(divertQueue));
      assertNotNull(server.getPostOffice().getBinding(divert));
   }

   @Test
   public void testRetroactiveResourceRemoval() throws Exception {
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertAddress = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.ADDRESS);
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      final SimpleString divert = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.DIVERT);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(10));

      server.addAddressInfo(new AddressInfo(addressName));
      assertNotNull(server.getAddressInfo(divertAddress));
      assertNotNull(server.locateQueue(divertQueue));
      assertNotNull(server.getPostOffice().getBinding(divert));

      server.removeAddressInfo(addressName, null, true);
      assertNull(server.getAddressInfo(divertAddress));
      assertNull(server.locateQueue(divertQueue));
      assertNull(server.getPostOffice().getBinding(divert));
   }

   @Test
   public void testRetroactiveAddress() throws Exception {
      final int COUNT = 25;
      final int LOOPS = 50;
      final SimpleString queueName = SimpleString.toSimpleString("simpleQueue");
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT));
      server.addAddressInfo(new AddressInfo(addressName));

      for (int i = 0; i < LOOPS; i++) {
         ClientProducer producer = session.createProducer(addressName);
         for (int j = 0; j < COUNT; j++) {
            ClientMessage message = session.createMessage(false);
            message.putIntProperty("xxx", (i * COUNT) + j);
            producer.send(message);
         }
         producer.close();

         final int finalI = i;
         Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessagesReplaced() == (COUNT * finalI), 3000, 50);
         Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == COUNT, 3000, 50);

         session.createQueue(addressName.toString(), RoutingType.ANYCAST, queueName.toString());
         Wait.assertTrue(() -> server.locateQueue(queueName) != null);
         Wait.assertTrue(() -> server.locateQueue(queueName).getMessageCount() == COUNT, 500, 50);
         ClientConsumer consumer = session.createConsumer(queueName);
         for (int j = 0; j < COUNT; j++) {
            session.start();
            ClientMessage message = consumer.receive(1000);
            assertNotNull(message);
            message.acknowledge();
            assertEquals((i * COUNT) + j, (int) message.getIntProperty("xxx"));
         }
         consumer.close();
         session.deleteQueue(queueName);
      }
   }

   @Test
   public void testRestart() throws Exception {
      final String data = "Simple Text " + UUID.randomUUID().toString();
      final SimpleString queueName1 = SimpleString.toSimpleString("simpleQueue1");
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(10));
      server.addAddressInfo(new AddressInfo(addressName));

      ClientProducer producer = session.createProducer(addressName);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString(data + "1");
      producer.send(message);
      producer.close();
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == 1, 500, 50);

      server.stop();
      server.start();
      assertNotNull(server.locateQueue(divertQueue));
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == 1, 500, 50);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(10));
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));

      producer = session.createProducer(addressName);
      message = session.createMessage(true);
      message.getBodyBuffer().writeString(data + "2");
      producer.send(message);
      producer.close();

      session.createQueue(addressName.toString(), RoutingType.ANYCAST, queueName1.toString());
      Wait.assertTrue(() -> server.locateQueue(queueName1) != null);
      Wait.assertTrue(() -> server.locateQueue(queueName1).getMessageCount() == 2, 500, 50);

      ClientConsumer consumer = session.createConsumer(queueName1);
      session.start();
      message = consumer.receive(1000);
      assertNotNull(message);
      message.acknowledge();
      assertEquals(data + "1", message.getBodyBuffer().readString());
      message = consumer.receive(1000);
      assertNotNull(message);
      message.acknowledge();
      assertEquals(data + "2", message.getBodyBuffer().readString());
      consumer.close();
      Wait.assertTrue(() -> server.locateQueue(queueName1).getMessageCount() == 0, 500, 50);

      Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == 2, 2000, 100);
   }

   @Test
   public void testUpdateAfterRestart() throws Exception {
      final int COUNT = 10;
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT));
      server.addAddressInfo(new AddressInfo(addressName));
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getRingSize() == COUNT, 1000, 100);
      server.stop();
      server.start();
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT * 2));
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getRingSize() == COUNT * 2, 1000, 100);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT));
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getRingSize() == COUNT, 1000, 100);
   }

   @Test
   public void testMulticast() throws Exception {
      final String data = "Simple Text " + UUID.randomUUID().toString();
      final SimpleString queueName1 = SimpleString.toSimpleString("simpleQueue1");
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(10));
      server.addAddressInfo(new AddressInfo(addressName));

      ClientProducer producer = session.createProducer(addressName);
      ClientMessage message = session.createMessage(false);
      message.getBodyBuffer().writeString(data);
      message.setRoutingType(RoutingType.MULTICAST);
      producer.send(message);
      producer.close();
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == 1, 500, 50);

      session.createQueue(addressName.toString(), RoutingType.MULTICAST, queueName1.toString());
      Wait.assertTrue(() -> server.locateQueue(queueName1) != null);
      Wait.assertTrue(() -> server.locateQueue(queueName1).getMessageCount() == 1, 500, 50);

      ClientConsumer consumer = session.createConsumer(queueName1);
      session.start();
      message = consumer.receive(1000);
      assertNotNull(message);
      message.acknowledge();
      assertEquals(data, message.getBodyBuffer().readString());
      consumer.close();
      Wait.assertTrue(() -> server.locateQueue(queueName1).getMessageCount() == 0, 500, 50);

      Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == 1, 2000, 100);
   }

   @Test
   public void testJMSTopicSubscribers() throws Exception {
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final int COUNT = 10;
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT));
      server.addAddressInfo(new AddressInfo(addressName));

      ConnectionFactory cf = new ActiveMQConnectionFactory("vm://0");
      Connection c = cf.createConnection();
      Session s = c.createSession();
      Topic t = s.createTopic(addressName.toString());

      MessageProducer producer = s.createProducer(t);
      for (int i = 0; i < COUNT * 2; i++) {
         Message m = s.createMessage();
         m.setIntProperty("test", i);
         producer.send(m);
      }
      producer.close();
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getMessageCount() == COUNT, 500, 50);

      MessageConsumer consumer = s.createConsumer(t);
      c.start();
      for (int i = 0; i < COUNT; i++) {
         Message m = consumer.receive(500);
         assertNotNull(m);
         assertEquals(i + COUNT, m.getIntProperty("test"));
      }
      assertNull(consumer.receiveNoWait());
   }

   @Test
   public void testUpdateAddressSettings() throws Exception {
      final int COUNT = 10;
      final SimpleString addressName = SimpleString.toSimpleString("myAddress");
      final SimpleString divertQueue = ResourceNames.getRetroactiveResourceName(server.getConfiguration().getInternalNamingPrefix(), addressName, ResourceNames.QUEUE);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT));
      server.addAddressInfo(new AddressInfo(addressName));
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT * 2));
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getRingSize() == COUNT * 2, 1000, 100);
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setRetroactiveMessageCount(COUNT));
      Wait.assertTrue(() -> server.locateQueue(divertQueue).getRingSize() == COUNT, 1000, 100);
   }
}
