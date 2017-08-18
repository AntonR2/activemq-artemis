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

package org.apache.activemq.artemis.utils;

public class LoggingUtil {

   /**
    * Utility for an exponential logging back-off
    *
    * Will return true on 1, tens, hundreds, thousands, ten-thousands, etc.
    *
    * @param count
    * @return
    */
   public static boolean shouldLog(int count) {
      if (count == 0) {
         return false;
      } else if (count == 1) {
         return true;
      } else {
         long i = 10;
         while (true) {
            if (i > count) {
               return false;
            } else if (count <= (i * 10) && (count % i == 0)) {
               return true;
            }
            i *= 10;
         }
      }
   }
}
