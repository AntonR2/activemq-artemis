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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A simple JMS Queue example that uses SSL secure transport.
 */
public class SSLExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         // ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         /*
          * Due to ARTEMIS-1846 the property "activemq.usemaskedpassword" doesn't work in a URL. The work-around is to
          * create the connection factory programmatically. The following code will programmatically create a connection
          * factory which is equivalent to the URL:
          * tcp://localhost:5500?sslEnabled=true&trustStorePath=activemq/server0/activemq.example.truststore&trustStorePassword=66179ddd354243477ee0128e1b0a13b8&activemq.usemaskedpassword=true
          */
         Map<String, Object> params = new HashMap<>();
         params.put("host", "127.0.0.1");
         params.put("port", 5500);
         params.put("sslEnabled", true);
         params.put("activemq.usemaskedpassword", true);
         params.put("trustStorePath", "activemq/server0/activemq.example.truststore");
         params.put("trustStorePassword", "66179ddd354243477ee0128e1b0a13b8");
         TransportConfiguration transport = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
         ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transport);

         // Step 4.Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());

         initialContext.close();
      } finally {
         // Step 12. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}
