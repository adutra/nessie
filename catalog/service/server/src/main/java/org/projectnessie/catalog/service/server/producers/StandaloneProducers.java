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
package org.projectnessie.catalog.service.server.producers;

import static java.lang.String.format;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackend;

/**
 * "Quick and dirty" producers providing connection to Nessie, a "storage" impl and object-store
 * I/O.
 */
public class StandaloneProducers {

  @Produces
  @Singleton
  public Backend backend() {
    return new InmemoryBackend();
  }

  @Produces
  @Singleton
  public Persist persist(Backend backend, StoreConfig config) {
    return backend.createFactory().newPersist(config);
  }

  @Produces
  @Singleton
  public StoreConfig storeConfig() {
    return new StoreConfig() {};
  }

  @Produces
  @Singleton
  public NessieApiV2 nessieApiV2() {
    int port = Integer.getInteger("nessie-core.port", 19120);
    return NessieClientBuilder.createClientBuilderFromSystemSettings()
        .withUri(format("http://127.0.0.1:%d/api/v2", port))
        .build(NessieApiV2.class);
  }
}
