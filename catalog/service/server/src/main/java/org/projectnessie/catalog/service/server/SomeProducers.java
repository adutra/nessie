/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.service.server;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.local.LocalObjectIO;
import org.projectnessie.catalog.service.common.config.CatalogServerConfig;
import org.projectnessie.catalog.service.common.config.ImmutableCatalogServerConfig;
import org.projectnessie.catalog.storage.backend.CatalogStorage;
import org.projectnessie.catalog.storage.persist.PersistCatalogStorage;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackend;

/**
 * "Quick and dirty" producers providing connection to Nessie, a "storage" impl and object-store
 * I/O.
 */
public class SomeProducers {
  @Produces
  @Singleton
  public CatalogServerConfig catalogServerConfig() {
    return ImmutableCatalogServerConfig.builder().sendStacktraceToClient(true).build();
  }

  @Produces
  @Singleton
  public NessieApiV2 nessieApiV2() {
    return NessieClientBuilder.createClientBuilderFromSystemSettings()
        .withUri("http://127.0.0.1:19120/api/v2")
        .build(NessieApiV2.class);
  }

  @Produces
  @Singleton
  public Backend backend() {
    return new InmemoryBackend();
  }

  @Produces
  @Singleton
  public StoreConfig storeConfig() {
    return new StoreConfig() {};
  }

  @Produces
  @Singleton
  public CatalogStorage catalogStorage(Backend backend, StoreConfig config) {
    return new PersistCatalogStorage(backend.createFactory().newPersist(config));
  }

  @Produces
  @Singleton
  public ObjectIO objectIO() {
    return new LocalObjectIO();
  }
}
