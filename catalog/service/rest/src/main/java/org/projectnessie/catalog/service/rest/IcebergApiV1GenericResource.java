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
package org.projectnessie.catalog.service.rest;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.PermitAll;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTransactionRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConfigResponse;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;

/**
 * Handles Iceberg REST API v1 endpoints that are not strongly associated with a particular entity
 * type.
 */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1GenericResource extends IcebergApiV1ResourceBase {

  @Inject IcebergConfigurer icebergConfigurer;
  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.UNKNOWN);
  }

  /** Exposes the Iceberg REST configuration for the Nessie default branch. */
  @Operation(operationId = "iceberg.v1.getConfig")
  @GET
  @Path("/v1/config")
  public IcebergConfigResponse getConfig(@QueryParam("warehouse") String warehouse) {
    return getConfig(null, warehouse);
  }

  /**
   * Exposes the Iceberg REST configuration for the named Nessie {@code reference} in the
   * {@code @Path} parameter.
   */
  @Operation(operationId = "iceberg.v1.getConfig.reference")
  @GET
  @Path("{reference}/v1/config")
  public IcebergConfigResponse getConfig(
      @PathParam("reference") String reference, @QueryParam("warehouse") String warehouse) {
    IcebergConfigResponse.Builder configResponse = IcebergConfigResponse.builder();
    icebergConfigurer.icebergWarehouseConfig(
        reference, warehouse, configResponse::putDefault, configResponse::putOverride);
    return configResponse.build();
  }

  @Operation(operationId = "iceberg.v1.getToken")
  @POST
  @Path("/v1/oauth/tokens")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @PermitAll
  public Response getToken() {
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            Map.of(
                "error",
                "NotImplemented",
                "error_description",
                "Endpoint not implemented: please configure "
                    + "the catalog client with the oauth2-server-uri property."))
        .build();
  }

  @Operation(operationId = "iceberg.v1.getToken.reference")
  @POST
  @Path("/{reference}/v1/oauth/tokens")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @PermitAll
  public Response getToken(@PathParam("reference") String ignored) {
    return getToken();
  }

  @Operation(operationId = "iceberg.v1.commitTransaction")
  @POST
  @Path("/v1/{prefix}/transactions/commit")
  @Blocking
  public Uni<Void> commitTransaction(
      @PathParam("prefix") String prefix,
      @Valid IcebergCommitTransactionRequest commitTransactionRequest)
      throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();

    CatalogCommit.Builder commit = CatalogCommit.builder();
    commitTransactionRequest.tableChanges().stream()
        .map(
            tableChange ->
                IcebergCatalogOperation.builder()
                    .updates(tableChange.updates())
                    .requirements(tableChange.requirements())
                    .contentKey(requireNonNull(tableChange.identifier()).toNessieContentKey())
                    .contentType(ICEBERG_TABLE)
                    .build())
        .forEach(commit::addOperations);

    SnapshotReqParams reqParams = SnapshotReqParams.forSnapshotHttpReq(ref, "iceberg", null);

    // Although we don't return anything, need to make sure that the commit operation starts and all
    // results are consumed.
    return Uni.createFrom()
        .completionStage(catalogService.commit(ref, commit.build(), reqParams))
        .map(stream -> stream.reduce(null, (ident, snap) -> ident, (i1, i2) -> i1));
  }
}
