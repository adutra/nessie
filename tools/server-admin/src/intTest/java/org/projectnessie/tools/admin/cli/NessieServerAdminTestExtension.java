/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.tools.admin.cli;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.bigtabletests.AbstractBigTableBackendTestFactory;
import org.projectnessie.versioned.storage.cassandratests.AbstractCassandraBackendTestFactory;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.dynamodbtests.DynamoDBBackendTestFactory;
import org.projectnessie.versioned.storage.jdbctests.AbstractJdbcBackendTestFactory;
import org.projectnessie.versioned.storage.mongodbtests.MongoDBBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;

/**
 * A JUnit5 extension that sets up the execution environment for Nessie CLI tests.
 *
 * <p>MongoDB storage is used by default, overridable by setting the system property {@code
 * backendTestFactoryClassName} to the fully qualified class name of the desired {@link
 * BackendTestFactory} implementation.
 *
 * <p>A {@link Persist} instance is created and injected into tests for manipulating the test Nessie
 * repository
 *
 * <p>The test Nessie repository is erased and re-created for each test case.
 */
public class NessieServerAdminTestExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {

  private static final Namespace NAMESPACE = Namespace.create(NessieServerAdminTestExtension.class);
  private static final String BACKEND_KEY = "backend";
  private static final String PERSIST_KEY = "adapter";
  private static final String BACKEND_TEST_FACTORY_CLASS_NAME_PROPERTY = "backendTestFactory";

  @Override
  public void beforeAll(ExtensionContext context) {
    BackendHolder backend = getOrCreateBackendHolder(context);

    // Quarkus runtime will pick up relevant values from java system properties.
    backend.config.forEach(System::setProperty);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    // Use one Persist instance for all tests (instances are stateless)
    Persist persist = getOrCreatePersist(context);
    // ... but reset the repo for each test.
    persist.erase();
    repositoryLogic(persist).initialize("main");
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(Persist.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    return getOrCreatePersist(context);
  }

  private Persist getOrCreatePersist(ExtensionContext context) {
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(PERSIST_KEY, key -> createPersist(context), Persist.class);
  }

  private BackendHolder getOrCreateBackendHolder(ExtensionContext context) {
    // Maintain one Backend instance for all tests (store it in the root context).
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(BACKEND_KEY, key -> createBackendHolder(), BackendHolder.class);
  }

  private Persist createPersist(ExtensionContext context) {
    BackendHolder backend = getOrCreateBackendHolder(context);
    PersistFactory persistFactory = backend.backend.createFactory();
    return persistFactory.newPersist(
        StoreConfig.Adjustable.empty().withRepositoryId(BaseConfigProfile.TEST_REPO_ID));
  }

  private BackendHolder createBackendHolder() {
    try {
      BackendTestFactory backendTestFactory = createBackendTestFactory();
      backendTestFactory.start();
      Backend backend = backendTestFactory.createNewBackend();
      backend.setupSchema();
      return new BackendHolder(backend, augmentQuarkusConfig(backendTestFactory));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, String> augmentQuarkusConfig(BackendTestFactory factory) {
    Map<String, String> config = new HashMap<>(factory.getQuarkusConfig());
    if (factory instanceof MongoDBBackendTestFactory) {
      config.put("nessie.version.store.type", VersionStoreType.MONGODB.name());
    } else if (factory instanceof DynamoDBBackendTestFactory) {
      config.put("nessie.version.store.type", VersionStoreType.DYNAMODB.name());
    } else if (factory instanceof AbstractBigTableBackendTestFactory) {
      config.put("nessie.version.store.type", VersionStoreType.BIGTABLE.name());
    } else if (factory instanceof AbstractJdbcBackendTestFactory f) {
      config.put("nessie.version.store.type", VersionStoreType.JDBC.name());
      config.put("nessie.version.store.persist.jdbc.datasource", f.getQuarkusDataSource());
    } else if (factory instanceof AbstractCassandraBackendTestFactory) {
      config.put("nessie.version.store.type", VersionStoreType.CASSANDRA.name());
    }
    return config;
  }

  private BackendTestFactory createBackendTestFactory() {
    String backendTestFactoryClassName =
        System.getProperty(BACKEND_TEST_FACTORY_CLASS_NAME_PROPERTY);
    if (backendTestFactoryClassName == null || backendTestFactoryClassName.isEmpty()) {
      return new MongoDBBackendTestFactory();
    }
    try {
      return (BackendTestFactory)
          Class.forName(backendTestFactoryClassName).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final class BackendHolder implements CloseableResource {
    final Backend backend;
    final Map<String, String> config;

    BackendHolder(Backend backend, Map<String, String> config) {
      this.backend = backend;
      this.config = config;
    }

    @Override
    public void close() throws Exception {
      backend.close();
    }
  }
}
