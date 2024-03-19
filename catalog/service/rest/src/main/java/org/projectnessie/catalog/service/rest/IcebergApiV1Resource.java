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

import static java.util.Collections.emptyList;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergError.icebergError;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorResponse.icebergErrorResponse;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.PermitAll;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.formats.iceberg.metrics.IcebergMetricsReport;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTransactionRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitViewRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConfigResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConflictException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateViewRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergGetNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListNamespacesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListTablesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadViewResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergOAuthTokenEndpointException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergOAuthTokenRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergOAuthTokenResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRegisterTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRestBaseException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateTableRequest;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.common.config.CatalogConfig;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ContentKeyErrorDetails;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieRuntimeException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.config.ExceptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements no logic, but the required, special exception handling when returning {@link Uni}s,
 * delegates to {@link IcebergApiV1ResourceBase}.
 */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1Resource extends IcebergApiV1ResourceBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergApiV1Resource.class);

  private static final String ENTITY_KIND_UNKNOWN = "Unknown";
  private static final String ENTITY_KIND_TABLE = "Table";
  private static final String ENTITY_KIND_VIEW = "View";
  private static final String ENTITY_KIND_NAMESPACE = "Namespace";

  private final ExceptionConfig exceptionConfig;

  @SuppressWarnings("unused")
  public IcebergApiV1Resource() {
    this(null, null, null, null, null, null, null);
  }

  @Inject
  public IcebergApiV1Resource(
      CatalogService catalogService,
      ObjectIO objectIO,
      RequestSigner signer,
      NessieApiV2 nessieApi,
      CatalogConfig catalogConfig,
      ExceptionConfig exceptionConfig,
      S3Options<S3BucketOptions> s3Options) {
    super(catalogService, objectIO, signer, nessieApi, catalogConfig, s3Options);
    this.exceptionConfig = exceptionConfig;
  }

  /** Exposes the Iceberg REST configuration for the Nessie default branch. */
  @GET
  @Path("/v1/config")
  @PermitAll
  public IcebergConfigResponse getConfig(@QueryParam("warehouse") String warehouse) {
    try {
      return super.getConfig(null, warehouse);
    } catch (RuntimeException e) {
      throw handleException(e, ENTITY_KIND_UNKNOWN);
    }
  }

  /**
   * Exposes the Iceberg REST configuration for the named Nessie {@code reference} in the
   * {@code @Path} parameter.
   */
  @GET
  @Path("{reference}/v1/config")
  @PermitAll
  public IcebergConfigResponse getConfig(
      @PathParam("reference") String reference, @QueryParam("warehouse") String warehouse) {
    try {
      return super.getConfig(reference, warehouse);
    } catch (RuntimeException e) {
      throw handleException(e, ENTITY_KIND_UNKNOWN);
    }
  }

  // TODO inject some path parameters to identify the table in a secure way to then do the
  //  access-check against the table and eventually sign the request.
  //  The endpoint can be tweaked sing the S3_SIGNER_ENDPOINT property, signing to be
  //  enabled via S3_REMOTE_SIGNING_ENABLED.
  @POST
  @Path("/v1/{prefix}/s3-sign/{identifier}")
  @Blocking
  public IcebergS3SignResponse s3sign(
      IcebergS3SignRequest request,
      @PathParam("prefix") String prefix,
      @PathParam("identifier") String identifier) {
    try {
      return super.s3sign(request, prefix, identifier);
    } catch (RuntimeException e) {
      throw handleException(e, ENTITY_KIND_UNKNOWN);
    }
  }

  @POST
  @Path("/v1/oauth/tokens")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @PermitAll
  public IcebergOAuthTokenResponse getToken(IcebergOAuthTokenRequest request)
      throws IcebergOAuthTokenEndpointException {
    try {
      return super.getToken(request);
    } catch (RuntimeException e) {
      throw handleException(e, ENTITY_KIND_UNKNOWN);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces")
  @Blocking
  public IcebergCreateNamespaceResponse createNamespace(
      @PathParam("prefix") String prefix,
      @Valid IcebergCreateNamespaceRequest createNamespaceRequest) {
    try {
      return super.createNamespace(prefix, createNamespaceRequest);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_NAMESPACE);
    }
  }

  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public void dropNamespace(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      super.dropNamespace(prefix, namespace);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_NAMESPACE);
    }
  }

  @GET
  @Path("/v1/{prefix}/namespaces")
  @Blocking
  public IcebergListNamespacesResponse listNamespaces(
      @PathParam("prefix") String prefix,
      @QueryParam("parent") String parent,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    try {
      return super.listNamespaces(prefix, parent, pageToken, pageSize);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_NAMESPACE);
    }
  }

  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public void namespaceExists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      super.namespaceExists(prefix, namespace);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_NAMESPACE);
    }
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public IcebergGetNamespaceResponse loadNamespaceMetadata(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      return super.loadNamespaceMetadata(prefix, namespace);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_NAMESPACE);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/properties")
  @Blocking
  public IcebergUpdateNamespacePropertiesResponse updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergUpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    try {
      return super.updateProperties(prefix, namespace, updateNamespacePropertiesRequest);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_NAMESPACE);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables")
  @Blocking
  public Uni<IcebergCreateTableResponse> createTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergCreateTableRequest createTableRequest,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess)
      throws IcebergConflictException {
    try {
      return handleException(
          super.createTable(prefix, namespace, createTableRequest, dataAccess), ENTITY_KIND_TABLE);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/register")
  @Blocking
  public Uni<IcebergLoadTableResponse> registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergRegisterTableRequest registerTableRequest)
      throws IcebergConflictException {
    try {
      return handleException(
          super.registerTable(prefix, namespace, registerTableRequest), ENTITY_KIND_TABLE);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public void dropTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("purgeRequested") @DefaultValue("false") Boolean purgeRequested) {
    try {
      super.dropTable(prefix, namespace, table, purgeRequested);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/tables")
  @Blocking
  public IcebergListTablesResponse listTables(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    try {
      return super.listTables(prefix, namespace, pageToken, pageSize);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public Uni<IcebergLoadTableResponse> loadTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("snapshots") String snapshots,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess) {
    try {
      return handleException(
          super.loadTable(prefix, namespace, table, snapshots, dataAccess), ENTITY_KIND_TABLE);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @POST
  @Path("/v1/{prefix}/tables/rename")
  @Blocking
  public void renameTable(
      @PathParam("prefix") String prefix,
      @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IcebergConflictException {
    try {
      super.renameTable(prefix, renameTableRequest);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public void tableExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    try {
      super.tableExists(prefix, namespace, table);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics")
  @Blocking
  public void reportMetrics(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @Valid @NotNull IcebergMetricsReport reportMetricsRequest) {
    try {
      super.reportMetrics(prefix, namespace, table, reportMetricsRequest);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public Uni<IcebergCommitTableResponse> updateTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @Valid IcebergUpdateTableRequest commitTableRequest)
      throws IcebergConflictException {
    try {
      return handleException(
          super.updateTable(prefix, namespace, table, commitTableRequest), ENTITY_KIND_TABLE);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @POST
  @Path("/v1/{prefix}/transactions/commit")
  @Blocking
  public Uni<Void> commitTransaction(
      @PathParam("prefix") String prefix,
      @Valid IcebergCommitTransactionRequest commitTransactionRequest)
      throws IcebergConflictException {
    try {
      return handleException(
          super.commitTransaction(prefix, commitTransactionRequest), ENTITY_KIND_TABLE);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_TABLE);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/views")
  @Blocking
  public Uni<IcebergLoadViewResponse> createView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergCreateViewRequest createViewRequest)
      throws IcebergConflictException {
    try {
      return handleException(
          super.createView(prefix, namespace, createViewRequest), ENTITY_KIND_VIEW);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public void dropView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @QueryParam("purgeRequested") @DefaultValue("false") Boolean purgeRequested) {
    try {
      super.dropView(prefix, namespace, view, purgeRequested);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/views")
  @Blocking
  public IcebergListTablesResponse listViews(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    try {
      return super.listViews(prefix, namespace, pageToken, pageSize);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public Uni<IcebergLoadViewResponse> loadView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    try {
      return handleException(super.loadView(prefix, namespace, view), ENTITY_KIND_VIEW);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  @POST
  @Path("/v1/{prefix}/views/rename")
  @Blocking
  public void renameView(
      @PathParam("prefix") String prefix,
      @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IcebergConflictException {
    try {
      super.renameView(prefix, renameTableRequest);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public void viewExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    try {
      super.viewExists(prefix, namespace, view);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public Uni<IcebergLoadViewResponse> updateView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @Valid IcebergCommitViewRequest commitViewRequest)
      throws IcebergConflictException {
    try {
      return handleException(
          super.updateView(prefix, namespace, view, commitViewRequest), ENTITY_KIND_VIEW);
    } catch (IOException | RuntimeException e) {
      throw handleException(e, ENTITY_KIND_VIEW);
    }
  }

  // Transactions

  private <R> Uni<R> handleException(Uni<R> async, String entityKind) {
    return async.onFailure().transform(ex -> handleException(ex, entityKind));
  }

  private IcebergErrorException handleException(Throwable ex, String entityKind) {
    if (ex instanceof CompletionException || ex.getClass() == RuntimeException.class) {
      ex = ex.getCause();
    }

    if (ex instanceof BaseNessieClientServerException) {
      BaseNessieClientServerException e = (BaseNessieClientServerException) ex;
      ErrorCode err = e.getErrorCode();
      return mapNessieErrorCodeException(e, err, e.getErrorDetails(), entityKind);
    } else if (ex instanceof NessieRuntimeException) {
      NessieRuntimeException e = (NessieRuntimeException) ex;
      ErrorCode err = e.getErrorCode();
      return mapNessieErrorCodeException(e, err, null, entityKind);
    } else if (ex instanceof IcebergRestBaseException) {
      IcebergRestBaseException icebergRest = (IcebergRestBaseException) ex;
      return genericErrorResponse(
          icebergRest.getResponseCode(), icebergRest.getType(), icebergRest.getMessage(), ex);
    } else if (ex instanceof IllegalArgumentException) {
      return genericErrorResponse(400, ex.getClass().getSimpleName(), ex.getMessage(), ex);
    } else {
      LOGGER.warn("Unhandled exception returned as HTTP/500", ex);
      return serverErrorException(ex);
    }
  }

  private IcebergErrorException genericErrorResponse(
      int code, String type, String message, Throwable ex) {
    List<String> stack;
    if (exceptionConfig.sendStacktraceToClient()) {
      stack =
          Arrays.stream(ex.getStackTrace())
              .map(StackTraceElement::toString)
              .collect(Collectors.toList());
    } else {
      stack = emptyList();
    }

    return new IcebergErrorException(
        icebergErrorResponse(icebergError(code, type, message, stack)), ex);
  }

  private IcebergErrorException mapNessieErrorCodeException(
      Exception ex, ErrorCode err, NessieErrorDetails errorDetails, String entityKind) {
    switch (err) {
      case UNSUPPORTED_MEDIA_TYPE:
      case BAD_REQUEST:
        return genericErrorResponse(400, "BadRequestException", ex.getMessage(), ex);
      case FORBIDDEN:
        return genericErrorResponse(403, "NotAuthorizedException", ex.getMessage(), ex);
      case CONTENT_NOT_FOUND:
        return genericErrorResponse(
            404,
            "NoSuch" + entityKind + "Exception",
            entityKind + " does not exist: " + keyMessage(ex, errorDetails),
            ex);
      case NAMESPACE_ALREADY_EXISTS:
        return genericErrorResponse(
            409,
            "AlreadyExistsException",
            "Namespace already exists: " + keyMessage(ex, errorDetails),
            ex);
      case NAMESPACE_NOT_EMPTY:
        return genericErrorResponse(409, "", ex.getMessage(), ex);
      case NAMESPACE_NOT_FOUND:
        return genericErrorResponse(
            404,
            "NoSuchNamespaceException",
            "Namespace does not exist: " + keyMessage(ex, errorDetails),
            ex);
      case REFERENCE_ALREADY_EXISTS:
        return genericErrorResponse(409, "", ex.getMessage(), ex);
      case REFERENCE_CONFLICT:
        if (ex instanceof NessieReferenceConflictException) {
          NessieReferenceConflictException referenceConflictException =
              (NessieReferenceConflictException) ex;
          ReferenceConflicts referenceConflicts = referenceConflictException.getErrorDetails();
          if (referenceConflicts != null) {
            List<Conflict> conflicts = referenceConflicts.conflicts();
            if (conflicts.size() == 1) {
              Conflict conflict = conflicts.get(0);
              ConflictType conflictType = conflict.conflictType();
              if (conflictType != null) {
                switch (conflictType) {
                  case NAMESPACE_ABSENT:
                    return genericErrorResponse(
                        404,
                        "NoSuchNamespaceException",
                        "Namespace does not exist: " + conflict.key(),
                        ex);
                  case KEY_DOES_NOT_EXIST:
                    return genericErrorResponse(
                        404,
                        "NoSuch" + entityKind + "Exception",
                        entityKind + " does not exist: " + ex.getMessage(),
                        ex);
                  default:
                    break;
                }
              }
            }
          }
        }
        return genericErrorResponse(409, "", ex.getMessage(), ex);
      case REFERENCE_NOT_FOUND:
        return genericErrorResponse(400, "NoSuchReferenceException", ex.getMessage(), ex);
      case UNKNOWN:
      case TOO_MANY_REQUESTS:
      default:
        break;
    }
    return serverErrorException(ex);
  }

  private static String keyMessage(Exception ex, NessieErrorDetails errorDetails) {
    if (errorDetails instanceof ContentKeyErrorDetails) {
      ContentKey key = ((ContentKeyErrorDetails) errorDetails).contentKey();
      if (key != null) {
        return key.toString();
      }
    }
    return ex.getMessage();
  }

  private IcebergErrorException serverErrorException(Throwable ex) {
    LOGGER.warn("Unhandled exception returned as HTTP/500", ex);
    return genericErrorResponse(500, ex.getClass().getSimpleName(), ex.getMessage(), ex);
  }
}
