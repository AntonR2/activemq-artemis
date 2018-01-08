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

import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public class ClusteredQueueExample {

   public static void main(final String[] args) throws Exception {
      JMSContext ctx0 = null;

      JMSContext ctx1 = null;

      try {

         // Step 1. Instantiate connection towards server 0
         ConnectionFactory cf0 = new JmsConnectionFactory("amqp://localhost:61616");

         // Step 2. Instantiate connection towards server 1
         ConnectionFactory cf1 = new JmsConnectionFactory("amqp://localhost:61617");

         // Step 3. We create a JMS Connection connection0 which is a connection to server 0
         ctx0 = cf0.createContext();

         // Step 4. We create a JMS Connection connection1 which is a connection to server 1
         ctx1 = cf1.createContext();

         // Step 5. We start the connections to ensure delivery occurs on them
         ctx1.start();

         // Step 6. We create JMS MessageConsumer objects on server 0 and server 1
         Queue queue = ctx0.createQueue("exampleQueue");

         JMSConsumer consumer1 = ctx1.createConsumer(queue);

         Thread.sleep(1000);

         // Step 7. We create a JMSProducer object on server 0
         JMSProducer producer = ctx0.createProducer();

         // Step 8. We send some messages to server 0
         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = ctx0.createTextMessage("This is text message " + i);

            producer.send(queue, message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 9. We now consume those messages on server 1
         for (int i = 0; i < numMessages; i++) {
            TextMessage message1 = (TextMessage) consumer1.receive(5000);

            System.out.println("Got message: " + message1.getText() + " from node 1");
         }
      } finally {
         // Step 10. Be sure to close our resources!

         if (ctx0 != null) {
            ctx0.close();
         }

         if (ctx1 != null) {
            ctx1.close();
         }
      }
   }
}
