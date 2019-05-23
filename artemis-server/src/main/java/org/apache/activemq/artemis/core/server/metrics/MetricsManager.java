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

package org.apache.activemq.artemis.core.server.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class MetricsManager {

   private String brokerName;

   private MeterRegistry meterRegistry;

   private Map<String, List<Meter>> meters = new HashMap<>();

   public MetricsManager(String brokerName, ActiveMQMetricsPlugin metricsPlugin) {
      this.brokerName = brokerName;
      meterRegistry = metricsPlugin.getRegistry();
      Metrics.globalRegistry.add(meterRegistry);
      new JvmMemoryMetrics().bindTo(meterRegistry);
   }

   public MeterRegistry getMeterRegistry() {
      return meterRegistry;
   }

   public void registerQueueGauge(String metricName, String address, String queue, Object object, ToDoubleFunction f, String description) {
      List<Meter> meters = this.meters.get(ResourceNames.QUEUE + queue);
      if (meters == null) {
         meters = new ArrayList<>();
         this.meters.put(ResourceNames.QUEUE + queue, meters);
      }
      Meter meter = Gauge
         .builder("artemis." + metricName, object, f)
         .tag("broker", brokerName)
         .tag("address", address)
         .tag("queue", queue)
         .description(description)
         .register(meterRegistry);

      meters.add(meter);

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Registered queue meter: " + meter.getId());
      }
   }

   public void registerAddressGauge(String metricName, String address, Object object, ToDoubleFunction f, String description) {
      List<Meter> meters = this.meters.get(ResourceNames.ADDRESS + address);
      if (meters == null) {
         meters = new ArrayList<>();
         this.meters.put(ResourceNames.ADDRESS + address, meters);
      }
      Meter meter = Gauge
         .builder("artemis." + metricName, object, f)
         .tag("broker", brokerName)
         .tag("address", address)
         .description(description)
         .register(meterRegistry);

      meters.add(meter);

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Registered address meter: " + meter.getId());
      }
   }

   public void registerBrokerGauge(String metricName, Object object, ToDoubleFunction f, String description) {
      List<Meter> meters = this.meters.get(ResourceNames.BROKER + "." + brokerName);
      if (meters == null) {
         meters = new ArrayList<>();
         this.meters.put(ResourceNames.BROKER + "." + brokerName, meters);
      }
      Meter meter = Gauge
         .builder("artemis." + metricName, object, f)
         .tag("broker", brokerName)
         .description(description)
         .register(meterRegistry);

      meters.add(meter);

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Registered broker meter: " + meter.getId());
      }
   }

   public void remove(String component) {
      List<Meter> meters = this.meters.remove(component);
      if (meters != null) {
         for (Meter meter : meters) {
            Meter removed = meterRegistry.remove(meter);
            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug("Removed meter: " + removed.getId());
            }
         }
      }
      meters.clear();
   }
}
