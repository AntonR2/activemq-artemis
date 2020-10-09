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
package org.apache.activemq.artemis.spi.core.security;

import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedRole;
import org.apache.activemq.artemis.core.persistence.config.PersistedUser;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.User;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.logs.AuditLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.apache.activemq.artemis.utils.SecurityManagerUtil;
import org.jboss.logging.Logger;

/**
 * All user and role state (both in memory and on disk) is maintained by the underlying StorageManager
 */
public class ActiveMQBasicSecurityManager implements ActiveMQSecurityManager5, UserManagement {

   private static final Logger logger = Logger.getLogger(ActiveMQBasicSecurityManager.class);

   public static final String BOOTSTRAP_USER = "bootstrapUser";
   public static final String BOOTSTRAP_PASSWORD = "bootstrapPassword";
   public static final String BOOTSTRAP_ROLE = "bootstrapRole";

   private Map<String, String> properties;
   private String rolePrincipalClass = RolePrincipal.class.getName();
   private StorageManager storageManager;

   @Override
   public ActiveMQBasicSecurityManager init(Map<String, String> properties) {
      if (!properties.containsKey(BOOTSTRAP_USER) || !properties.containsKey(BOOTSTRAP_PASSWORD) || !properties.containsKey(BOOTSTRAP_ROLE)) {
         ActiveMQServerLogger.LOGGER.noBootstrapCredentialsFound();
      } else {
         this.properties = properties;
      }
      return this;
   }

   @Override
   public boolean validateUser(String user, String password) {
      throw new UnsupportedOperationException("Invoke authenticate(String, String, RemotingConnection, String) instead");
   }

   @Override
   public Subject authenticate(final String userToAuthenticate, final String passwordToAuthenticate, RemotingConnection remotingConnection, final String securityDomain) {
      try {
         if (storageManager.getPersistedUsers() != null) {
            PersistedUser persistedUser = storageManager.getPersistedUsers().get(userToAuthenticate);
            if (persistedUser != null) {
               User user = new User(persistedUser.getUsername(), persistedUser.getPassword());
               if (user.isValid(userToAuthenticate, passwordToAuthenticate)) {
                  Subject subject = new Subject();
                  subject.getPrincipals().add(new UserPrincipal(userToAuthenticate));
                  for (String role : storageManager.getPersistedRoles().get(userToAuthenticate).getRoles()) {
                     subject.getPrincipals().add((Principal) SecurityManagerUtil.createGroupPrincipal(role, rolePrincipalClass));
                  }
                  if (AuditLogger.isAnyLoggingEnabled() && remotingConnection != null) {
                     remotingConnection.setAuditSubject(subject);
                  }
                  if (AuditLogger.isResourceLoggingEnabled()) {
                     AuditLogger.userSuccesfullyLoggedInAudit(subject);
                  }
                  return subject;
               }
            }
         }
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("Couldn't validate user", e);
         }
      }

      return null;
   }

   @Override
   public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
      throw new UnsupportedOperationException("Invoke authorize(Subject, Set<Role>, CheckType, String) instead");
   }

   @Override
   public boolean authorize(final Subject subject,
                            final Set<Role> roles,
                            final CheckType checkType,
                            final String address) {
      boolean authorized = SecurityManagerUtil.authorize(subject, roles, checkType, rolePrincipalClass);

      if (logger.isTraceEnabled()) {
         logger.trace("user " + (authorized ? " is " : " is NOT ") + "authorized");
      }

      return authorized;
   }

   @Override
   public synchronized void addNewUser(String user, String password, String... roles) throws Exception {
      if (user == null) {
         throw ActiveMQMessageBundle.BUNDLE.nullUser();
      }
      if (password == null) {
         throw ActiveMQMessageBundle.BUNDLE.nullPassword();
      }
      if (userExists(user)) {
         throw ActiveMQMessageBundle.BUNDLE.userAlreadyExists(user);
      }

      storageManager.storeUser(new PersistedUser(user, password));
      storageManager.storeRole(new PersistedRole(user, Arrays.asList(roles)));
   }

   @Override
   public synchronized void removeUser(final String user) throws Exception {
      if (!userExists(user)) {
         throw ActiveMQMessageBundle.BUNDLE.userDoesNotExist(user);
      }

      storageManager.deleteUser(user);
      storageManager.deleteRole(user);
   }

   @Override
   public synchronized Map<String, Set<String>> listUser(String user) {
      if (!userExists(user)) {
         throw ActiveMQMessageBundle.BUNDLE.userDoesNotExist(user);
      }

      Map<String, Set<String>> result = new HashMap<>();

      if (user != null && user.length() > 0) {
         result.put(user, new HashSet<>(storageManager.getPersistedRoles().get(user).getRoles()));
      } else {
         for (String thisUser : storageManager.getPersistedUsers().keySet()) {
            result.put(thisUser, new HashSet<>(storageManager.getPersistedRoles().get(thisUser).getRoles()));
         }
      }
      return result;
   }

   @Override
   public synchronized void updateUser(String user, String password, String... roles) throws Exception {
      if (!userExists(user)) {
         throw ActiveMQMessageBundle.BUNDLE.userDoesNotExist(user);
      }

      // potentially update the user's password
      if (password != null) {
         storageManager.deleteUser(user);
         storageManager.storeUser(new PersistedUser(user, password));
      }

      // potentially update the user's role(s)
      if (roles != null && roles.length > 0) {
         storageManager.deleteRole(user);
         storageManager.storeRole(new PersistedRole(user, Arrays.asList(roles)));
      }
   }

   public void setStorageManager(StorageManager storageManager) {
      this.storageManager = storageManager;

      // add/update the bootstrap user now that the StorageManager is set
      if (properties != null && properties.containsKey(BOOTSTRAP_USER) && properties.containsKey(BOOTSTRAP_PASSWORD) && properties.containsKey(BOOTSTRAP_ROLE)) {
         try {
            if (userExists(properties.get(BOOTSTRAP_USER))) {
               updateUser(properties.get(BOOTSTRAP_USER), properties.get(BOOTSTRAP_PASSWORD), new String[]{properties.get(BOOTSTRAP_ROLE)});
            } else {
               addNewUser(properties.get(BOOTSTRAP_USER), properties.get(BOOTSTRAP_PASSWORD), new String[]{properties.get(BOOTSTRAP_ROLE)});
            }
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.failedToCreateBootstrapCredentials(e, properties.get(BOOTSTRAP_USER));
         }
      }
   }

   private boolean userExists(String user) {
      return user != null && storageManager.getPersistedUsers() != null && storageManager.getPersistedUsers().containsKey(user);
   }
}
