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

package org.apache.activemq.artemis.api.core.management;

import java.util.UUID;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.Assert;
import org.junit.Test;

public class ResourceNamesTest extends Assert {

   final String prefix = ActiveMQDefaultConfiguration.getInternalNamingPrefix();
   final String testAddress = UUID.randomUUID().toString();
   final String testResourceName = prefix + testAddress + "." + ResourceNames.ADDRESS + ResourceNames.RETROACTIVE_SUFFIX;

   @Test
   public void testGetRetroactiveResourceName() {
      assertEquals(testResourceName, ResourceNames.getRetroactiveResourceName(prefix, SimpleString.toSimpleString(testAddress), ResourceNames.ADDRESS).toString());
   }

   @Test
   public void testDecomposeRetroactiveResourceName() {
      assertEquals(testAddress, ResourceNames.decomposeRetroactiveResourceName(prefix, testResourceName, ResourceNames.ADDRESS));
   }

   @Test
   public void testIsRetroactiveResource() {
      assertTrue(ResourceNames.isRetroactiveResource(prefix, SimpleString.toSimpleString(testResourceName), ResourceNames.ADDRESS));
   }
}
