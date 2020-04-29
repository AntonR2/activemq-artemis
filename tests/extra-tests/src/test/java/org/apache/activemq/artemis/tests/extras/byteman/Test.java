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
package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class Test extends JMSTestBase {
   private static final Logger log = Logger.getLogger(Test.class);

   private static Connection connection = null;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

   }

   protected ConnectionFactory getCF() throws Exception {
      return cf;
   }

   @org.junit.Test
   @BMRules(
      rules = {@BMRule(
         name = "test",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl",
         targetMethod = "createSessionChannel",
         targetLocation = "EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.Test.closeConnection();")})
   public void testCreateSessionAndCloseConnectionConcurrently() throws Exception {
      try {
         ActiveMQConnectionFactory fact = (ActiveMQConnectionFactory) getCF();
         connection = fact.createConnection();
         try {
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         } catch (JMSException e) {
            // ignore
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   public static void closeConnection() {
      try {
         if (connection != null) {
            System.out.println("Closing connection");
            connection.close();
         }
      } catch (JMSException e) {
         e.printStackTrace();
      }
   }
}
