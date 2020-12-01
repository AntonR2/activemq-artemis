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
package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IngressTimeTest extends ActiveMQTestBase {
   private ActiveMQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      server.start();
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setEnableIngressTime(true));
      server.createQueue(new QueueConfiguration(QUEUE).setRoutingType(RoutingType.ANYCAST));
   }

   @Test
   public void testSendCoreReceiveCore() throws Throwable {
      internalSendReceive(Protocol.CORE, Protocol.CORE);
   }

   @Test
   public void testSendAMQPReceiveCore() throws Throwable {
      internalSendReceive(Protocol.AMQP, Protocol.CORE);
   }

   @Test
   public void testSendOpenWireReceiveCore() throws Throwable {
      internalSendReceive(Protocol.OPENWIRE, Protocol.CORE);
   }

   @Test
   public void testSendCoreReceiveOpenwire() throws Throwable {
      internalSendReceive(Protocol.CORE, Protocol.OPENWIRE);
   }

   @Test
   public void testSendAMQPReceiveOpenWire() throws Throwable {
      internalSendReceive(Protocol.AMQP, Protocol.OPENWIRE);
   }

   @Test
   public void testSendOpenWireReceiveOpenWire() throws Throwable {
      internalSendReceive(Protocol.OPENWIRE, Protocol.OPENWIRE);
   }

   /**
    *  AMQP receiver tests are in org.apache.activemq.artemis.tests.integration.amqp.AmqpIngressTimeTest
    */

   private void internalSendReceive(Protocol protocolSender, Protocol protocolConsumer) throws Throwable {
      ConnectionFactory factorySend = createFactory(protocolSender);
      ConnectionFactory factoryConsume = protocolConsumer == protocolSender ? factorySend : createFactory(protocolConsumer);

      long beforeSend, afterSend;
      try (Connection connection = factorySend.createConnection()) {
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(QUEUE.toString());
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.setDeliveryMode(DeliveryMode.PERSISTENT);
               TextMessage msg = session.createTextMessage("hello");
               beforeSend = System.currentTimeMillis();
               producer.send(msg);
               afterSend = System.currentTimeMillis();
            }
         }
      }

      server.stop();
      server.start();
      assertTrue(server.waitForActivation(3, TimeUnit.SECONDS));

      try (Connection connection = factoryConsume.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(QUEUE.toString());
            try (MessageConsumer consumer = session.createConsumer(queue)) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               Assert.assertNotNull(message);
               Object ingressTime = message.getObjectProperty(Message.HDR_INGRESS_TIME.toString());
               assertNotNull(ingressTime);
               assertTrue(ingressTime instanceof Long);
               long ingress = (Long) ingressTime;
               assertTrue("Ingress time should be set in expected range",beforeSend <= ingress && afterSend >= ingress);
            }
         }
      }
   }

   private ConnectionFactory createFactory(Protocol protocol) {
      switch (protocol) {
         case CORE: ActiveMQConnectionFactory coreCF = new ActiveMQConnectionFactory();// core protocol
            coreCF.setCompressLargeMessage(true);
            coreCF.setMinLargeMessageSize(10 * 1024);
            return coreCF;
         case AMQP: return new JmsConnectionFactory("amqp://localhost:61616"); // amqp
         case OPENWIRE: return new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616"); // openwire
         default: return null;
      }
   }

   private enum Protocol {
      CORE, AMQP, OPENWIRE
   }
}
