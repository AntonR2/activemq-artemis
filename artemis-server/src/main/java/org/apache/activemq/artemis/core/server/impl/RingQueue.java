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
package org.apache.activemq.artemis.core.server.impl;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.jboss.logging.Logger;

public class RingQueue extends QueueImpl {

   private static final Logger logger = Logger.getLogger(RingQueue.class);
   private final long ringSize;

   public RingQueue(final long persistenceID,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final PageSubscription pageSubscription,
                    final SimpleString user,
                    final boolean durable,
                    final boolean temporary,
                    final boolean autoCreated,
                    final RoutingType routingType,
                    final Integer maxConsumers,
                    final Boolean exclusive,
                    final Boolean groupRebalance,
                    final Integer groupBuckets,
                    final SimpleString groupFirstKey,
                    final Integer consumersBeforeDispatch,
                    final Long delayBeforeDispatch,
                    final Boolean purgeOnNoConsumers,
                    final Long ringSize,
                    final Boolean nonDestructive,
                    final Boolean autoDelete,
                    final Long autoDeleteDelay,
                    final Long autoDeleteMessageCount,
                    final boolean configurationManaged,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                    final ArtemisExecutor executor,
                    final ActiveMQServer server,
                    final QueueFactory factory) {
      super(persistenceID, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, routingType, maxConsumers, exclusive, groupRebalance, groupBuckets, groupFirstKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, purgeOnNoConsumers, autoDelete, autoDeleteDelay, autoDeleteMessageCount, configurationManaged, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
      this.ringSize = ringSize;
   }

   @Override
   public synchronized void addTail(final MessageReference ref, final boolean direct) {
      enforceRing();

      super.addTail(ref, direct);
   }

   @Override
   public synchronized void addHead(final MessageReference ref, boolean scheduling) {
      enforceRing(ref);

      if (!ref.isAlreadyAcked()) {
         super.addHead(ref, scheduling);
      }
   }

   @Override
   public boolean allowsReferenceCallback() {
      return false;
   }

   private void enforceRing() {
      enforceRing(null);
   }

   private void enforceRing(MessageReference refToAck) {
      logger.debug("Checking ring size");
      if (getMessageCountForRing() >= ringSize) {
         refToAck = refToAck == null ? messageReferences.poll() : refToAck;

         if (refToAck != null) {
            logger.debugf("Preserving ringSize %d by acking message ref %s", ringSize, refToAck);
            referenceHandled(refToAck);

            try {
               refToAck.acknowledge(null, AckReason.REPLACED, null);
               if (!refToAck.isAlreadyDelivered()) {
                  refRemoved(refToAck);
               } else {
                  refToAck.setAlreadyDelivered(false);
               }
               refToAck.setAlreadyAcked();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
            }
         } else {
            logger.debugf("Cannot preserve ringSize %d; message ref is null", ringSize);
         }
      }
   }

   @Override
   public long getRingSize() {
      return ringSize;
   }

   private long getMessageCountForRing() {
      return (long) pendingMetrics.getMessageCount() + (pageSubscription == null ? 0 : pageSubscription.getMessageCount());
   }
}
