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

import java.util.ArrayList;
import java.util.UUID;

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

      System.out.println("Construct the large message...");
      byte[] content = buildContent();

      System.out.println("Publish Paho MQTT messages.");
      for (int idx = 0; idx < 200; idx++) {
         MqttMessage msg = new MqttMessage(content);
         publisherClient.mqttClient.publish(topicPaho1, msg);
         System.out.println("Paho MQTT message " + idx + " sent.");
         Thread.sleep(1500);
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

   public static byte[] buildContent() {

      ArrayList<String> stringval2 = buildContentArray();
      int size = 0;
      for (String value : stringval2) {
         size += value.length();
      }
      System.out.println();
      System.out.println("BuildContent ===== size = " + size);
      System.out.println();
      StringBuilder builder = new StringBuilder(size);
      for (String value : stringval2) {
         builder.append(value);
      }
      String msgContent = builder.toString();

      return msgContent.getBytes();
   }

   public static ArrayList<String> buildContentArray() {
      ArrayList<String> val = new ArrayList<>();
      String msgHdr = "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Header/><SOAP-ENV:Body><ns5:ExchangeMessage xmlns:ns5=\"urn:dpcl:wsdl:2011-09-02\" xmlns:ns3=\"http://www.w3.org/2004/08/xop/include\" xmlns:ns6=\"urn:dpcl:wsdl:2010-01-19\" xmlns:xmime=\"http://www.w3.org/2005/05/xmlmime\" xmlns=\"\"><ExchangeMessageInput><data xmime:contentType=\"application/vnd.dpcl.update_transfer+xml\"><base64>";
      String msgChunk = "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiIHN0YW5kYWxvbmU9InllcyI/Pgo8bnMyOlRyYW5zZmVyIHhtbG5zOm5zMj0idXJuOmRwY2w6dXBkYXRlOjIwMTEtMTAtMTkiPgogICAgPGltYWdlU2VnbWVudD4KICAgICAgICA8Ym9hcmQ+MjU5PC9ib2FyZD4KICAgICAgICA8Y2F0ZWdvcnk+MjwvY2F0ZWdvcnk+CiAgICAgICAgPHZlcnNpb24+Mjg1NDA5Mjg1PC92ZXJzaW9uPgogICAgICAgIDxpZD4yNjwvaWQ+CiAgICAgICAgPHNpemU+MjA5NzE1Mjwvc2l6ZT4KICAgICAgICA8Y2hlY2tzdW0+NTE0ODI3MGJmZTM2ZmYzNmIyZTNmMjc0NWJlNmYyMGY8L2NoZWNrc3VtPgogICAgICAgIDxkYXRhPm5OQUJ1WHQvWG0xYlhGeC9aallZbEJ1K2NrWU1ncHBTMnZpTVZoOUxjTENjTFlTL1Z6YUxlSWNnWmtlMjI5Z1dlS1p6czlSclBrdVlsSHYvaWNlSldJeTUxaGFpVUx3NTY0NWtTTUlhMEhjNnZoYTB5UC91OEVNUEcvck9LL1JhVXpuS0tRdXF5WVNDVlZ3TWROS25IWjZ5Sm91TkdMcVJ3a0MvVDZUdStrTWxKak9TcjV6MUNYWDdtZWdvSGpLdkFuU1AyOFJWY0F3MWVXTUtIY0pQU0Z0bFZXSkFYVXErZjFzbE9HWXlNSGhiN2haV0VnMWc4TlRlVUJ2NHJGL0RtUitKRjRmbjlWdkRJSkJYanJpeE5CNWFyc1RKOTR3dEF2YWxVM28vVzVnODltbURNNHp0VlVuaHZvSlRTSlZ6bXlqTGpJMWQ5OExVVTVWU3dqWE5KMjZ2d0F4R1ptVmwrVGlMU0JaeWNYak45NlYxVUZ6eldOMStPN2h5SHRMZnMvOE9kRjVMK1ArbjZXOXNqNVA3aDdGZUU4UFVHbGpLcXhxWmFGbFZ4aXJPRjYrUExGTHFFMzAzUzVodzJPeDFBQjA5Sjl4VThjVXNtUVI0dlJBS3B0Y3ZpbXkzb1VncmxWQTBwNG83cFdlYkduak1kT1N6ZGR2M01uMi9rMldlOVRHNzI3OEhkdTdLQlNtVW95VTJSM0l6TitITXhXeGQ4";

      val.add(msgHdr);
      for (int idx = 0; idx < 300; idx++) {
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
         val.add(msgChunk);
      }
      return val;
   }
}