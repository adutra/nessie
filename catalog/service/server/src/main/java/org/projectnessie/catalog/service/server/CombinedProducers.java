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
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.nessie.combined.CombinedClientBuilder;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * "Quick and dirty" producers providing connection to Nessie, a "storage" impl and object-store
 * I/O.
 */
public class CombinedProducers {

  @Produces
  @Singleton
  public NessieApiV2 nessieApiV2(Persist persist) {
    return new CombinedClientBuilder().withPersist(persist).build(NessieApiV2.class);
  }
}
