/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

public class AmqpIngressTimeTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testIngressTimestampSendCore() throws Exception {
      internalTestIngressTimestamp(Protocol.CORE);
   }

   @Test(timeout = 60000)
   public void testIngressTimestampSendAMQP() throws Exception {
      internalTestIngressTimestamp(Protocol.AMQP);
   }

   @Test(timeout = 60000)
   public void testIngressTimestampSendOpenWire() throws Exception {
      internalTestIngressTimestamp(Protocol.OPENWIRE);
   }

   private void internalTestIngressTimestamp(Protocol protocol) throws Exception {
      server.getAddressSettingsRepository().addMatch(getQueueName(), new AddressSettings().setEnableIngressTime(true));
      long beforeSend = System.currentTimeMillis();
      if (protocol == Protocol.CORE) {
         sendMessagesCore(getQueueName(), 1, true);
      } else if (protocol == Protocol.OPENWIRE) {
         sendMessagesOpenWire(getQueueName(), 1, true);
      } else {
         sendMessages(getQueueName(), 1, true);
      }
      long afterSend = System.currentTimeMillis();

      server.stop();
      server.start();
      assertTrue(server.waitForActivation(3, TimeUnit.SECONDS));

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      Queue queueView = getProxyToQueue(getQueueName());
      assertEquals(1, queueView.getMessageCount());

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      instanceLog.info(receive);
      Object ingressTimestamp = receive.getMessageAnnotation(AMQPMessageSupport.HDR_INGRESS_TIME);
      assertNotNull(ingressTimestamp);
      assertTrue(ingressTimestamp instanceof Long);
      long ingressTs = (Long) ingressTimestamp;
      assertTrue("Ingress time should be set in expected range",beforeSend <= ingressTs && afterSend >= ingressTs);
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   private enum Protocol {
      CORE, AMQP, OPENWIRE
   }
}
