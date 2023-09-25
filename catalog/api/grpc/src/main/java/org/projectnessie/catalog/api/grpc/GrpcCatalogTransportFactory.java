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
package org.projectnessie.catalog.api.grpc;

import io.grpc.Channel;
import java.util.Optional;
import org.projectnessie.catalog.api.base.NessieCatalogClientConfig;
import org.projectnessie.catalog.api.base.transport.CatalogTransport;
import org.projectnessie.catalog.api.base.transport.CatalogTransportFactory;
import org.projectnessie.catalog.api.proto.NessieCatalogServiceGrpc;
import org.projectnessie.catalog.api.proto.NessieCatalogServiceGrpc.NessieCatalogServiceStub;

public class GrpcCatalogTransportFactory
    implements CatalogTransportFactory<GrpcCatalogTransportConfig> {

  static final String NAME = "gRPC";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public CatalogTransport createTransport(
      NessieCatalogClientConfig stringStringMap,
      Optional<GrpcCatalogTransportConfig> transportConfig) {
    Channel channel = null;
    NessieCatalogServiceStub catalogService = NessieCatalogServiceGrpc.newStub(channel);
    return new GrpcCatalogTransport(catalogService);
  }

  @Override
  public Optional<Class<GrpcCatalogTransportConfig>> configMappingType() {
    return Optional.of(GrpcCatalogTransportConfig.class);
  }
}
