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
package org.projectnessie.catalog.api.base.transport;

import org.projectnessie.catalog.model.id.NessieId;

/**
 * Used by clients to give the catalog server a hint which "describing objects" it already knows, so
 * that a server only needs to return objects that a client does not know, also reducing load on the
 * server side.
 *
 * <p>Clients may cache individual "describing objects" (like schemas, partition specs or other
 * types that are immutable). Those objects do not need to be transferred from a server back to the
 * client.
 *
 * <p>Clients do not send all {@link NessieId IDs} including their types but a bloom-filter via
 * which a server can figure out whether the full objects need to be returned or whether the ID is
 * sufficient.
 *
 * <p>To mitigate bloom-filter false-positives and the cases when a cached descriptor was evicted in
 * the mean-time, a client can request missing "describing objects".
 *
 * <p>{@link CatalogTransportFactory Catalog transports} provide special implementations, suitable
 * for the transport, because the payload is just binary data and should be encoded efficiently.
 */
public interface KnownDescriptors {
  boolean maybeContains(NessieId id);

  boolean isEmpty();

  interface Builder {
    Builder addId(NessieId id);

    KnownDescriptors build();
  }
}
