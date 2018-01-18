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
package org.apache.activemq.artemis.mqtt.example;

import java.io.File;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.fusesource.mqtt.client.MQTT;

public class MQTTBasicPubSubExample2 implements MqttCallback {
   private MqttAsyncClient mqttClient;
   private MqttConnectOptions connOpts;
   protected static MQTTBasicPubSubExample2 publisherClient;
   protected static MQTTBasicPubSubExample2 consumerClient;

   private static String topicPaho1 = "State/PRN/";
   private static String topicPaho2  = "Soap/PRN/";
   public String name;

   public static void main(final String[] args) throws Exception {
      publisherClient = new MQTTBasicPubSubExample2();
      publisherClient.connect();
      publisherClient.name = "Pub";
      consumerClient = new MQTTBasicPubSubExample2();
      consumerClient.connect();
      consumerClient.name = "Consumer";
      System.out.println("Connecting to Artemis using MQTT");
      MQTT mqtt = new MQTT();
      mqtt.setHost("tcp://localhost:1883");

      System.out.println("Connected to Artemis");

      File file = new File("step1.txt");
      System.out.println("Search location for large message = " + file.getAbsolutePath());
      file = file.getAbsoluteFile();
      byte[] content = FileUtils.readFileToByteArray(file);
      System.out.println("Large message read successful = " + file.getAbsolutePath());

      System.out.println("Publish Paho MQTT messages.");
      for (int idx = 0; idx < 100; idx++) {
         MqttMessage msg = new MqttMessage(content);
         publisherClient.mqttClient.publish(topicPaho1 , msg);
         System.out.println("Paho MQTT message " + idx + " sent.");
         Thread.sleep(1000);
      }

      System.out.println("PubSubExample complete.");
   }

   public void connect() {
      // create a new Paho MqttClient
      MemoryPersistence persistence = new MemoryPersistence();
      // establish the client ID for the life of this DPI publisherClient
      String clientId = UUID.randomUUID().toString();
      try {
         mqttClient = new MqttAsyncClient("tcp://localhost:1883", clientId, persistence);
         // Create a set of connection options
         connOpts = new MqttConnectOptions();
         connOpts.setCleanSession(true);
         mqttClient.connect(connOpts);
      } catch (MqttException e) {
         e.printStackTrace();
      }
      // pause a moment to get connected (prevents the race condition)
      try {
         Thread.sleep(1000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }

      // subscribe
      try {
         String[] topicsPaho = new String[]{topicPaho1, topicPaho2};
         int[] qos = new int[]{0, 0};
         mqttClient.subscribe(topicsPaho, qos);
      } catch (MqttException e) {
         e.printStackTrace();
      }

      System.out.println("Subscribed to topics.");
      try {
         Thread.sleep(1000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      mqttClient.setCallback(this);
   }

   protected MQTTBasicPubSubExample2() {
      System.out.println("Client to Artemis using Paho MQTT");
   }

   @Override
   public void connectionLost(Throwable throwable) {
      System.out.println("Connection Lost! ...");
   }

   @Override
   public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {

      if (topic.startsWith(topicPaho1)) {
         // messages for this topic have arrived
         System.out.println("Message for topic arrived... " + topicPaho1);
      } else if (topic.startsWith(topicPaho2)) {
         // messages for this topic arrived
         System.out.println("Message for topic arrived... " + topicPaho2);
      }
      System.out.println(name + " ===== PubSubExample::messageArrived");
   }

   @Override
   public void deliveryComplete(IMqttDeliveryToken token) {
      System.out.println(name + " ===== PubSubExample::deliveryComplete...");
   }
}
