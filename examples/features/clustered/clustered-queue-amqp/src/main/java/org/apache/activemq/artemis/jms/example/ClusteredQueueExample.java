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
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster.
 */
public class ClusteredQueueExample {

   public static void main(final String[] args) throws Exception {
      // set this to true to use AMQP, false to use CORE
      final boolean AMQP = true;
      final int BROKER_COUNT = 6;
      final int MESSAGE_COUNT = 100;
      Connection[] connections = new Connection[BROKER_COUNT];

      try {
         Queue queue = null;
         ConnectionFactory[] cfs = new ConnectionFactory[BROKER_COUNT];
         Session[] sessions = new Session[BROKER_COUNT];
         MessageConsumer[] consumers = new MessageConsumer[BROKER_COUNT];

         for (int i = 0; i < BROKER_COUNT; i++) {
            String url = (AMQP ? "amqp" : "tcp") + "://localhost:616" + Integer.toString(16 + i);
            System.out.println("Connecting to " + url + "...");
            cfs[i] = AMQP ? new JmsConnectionFactory(url) : new ActiveMQConnectionFactory(url);
            connections[i] = cfs[i].createConnection();
            sessions[i] = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = sessions[i].createQueue("exampleQueue");
            connections[i].start();
            consumers[i] = sessions[i].createConsumer(queue);
         }

         Thread.sleep(2000);

         // Step 12. We create a JMS MessageProducer object on server 0
         MessageProducer producer = sessions[0].createProducer(queue);

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         // Step 13. We send some messages to server 0
         for (int i = 0; i < MESSAGE_COUNT * BROKER_COUNT; i++) {
            TextMessage message = sessions[0].createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 14. We now consume those messages on *both* server 0 and server 1.
         // We note the messages have been distributed between servers in a round robin fashion
         // JMS Queues implement point-to-point message where each message is only ever consumed by a
         // maximum of one consumer

         for (int i = 0; i < MESSAGE_COUNT; i++) {
            for (int j = 0; j < BROKER_COUNT; j++) {
               TextMessage message = (TextMessage) consumers[j].receive(2000);

               System.out.println("Got message: " + message.getText() + " from node " + j);
            }
         }
      } finally {
         // Step 15. Be sure to close our resources!
         for (int i = 0; i < BROKER_COUNT; i++) {
            if (connections[i] != null) {
               connections[i].close();
            }
         }
      }
   }
}
