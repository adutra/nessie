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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.unpartitioned;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.unsorted;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier.fromNessieContentKey;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec.addPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder.addSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddViewVersion.addViewVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentViewVersion.setCurrentViewVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultPartitionSpec.setDefaultPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultSortOrder.setDefaultSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation.setLocation;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse.icebergS3SignResponse;
import static org.projectnessie.catalog.service.rest.DecodedPrefix.decodedPrefix;
import static org.projectnessie.catalog.service.rest.NamespaceRef.namespaceRef;
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.api.sign.SigningRequest;
import org.projectnessie.catalog.api.sign.SigningResponse;
import org.projectnessie.catalog.api.types.CatalogCommit;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.metrics.IcebergMetricsReport;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
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
import org.projectnessie.catalog.formats.iceberg.rest.IcebergGetNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListNamespacesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListTablesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResult;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadViewResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRegisterTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateTableRequest;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.common.config.CatalogConfig;
import org.projectnessie.catalog.service.common.config.WarehouseConfig;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.PagingBuilder;
import org.projectnessie.client.api.UpdateNamespaceResult;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class IcebergApiV1ResourceBase extends AbstractCatalogResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergApiV1ResourceBase.class);

  /**
   * This is the signer endpoint (like {@code v1/aws/s3/sign}), it is evaluated per {@code S3FileIO}
   * instance.
   */
  public static final String S3_SIGNER_ENDPOINT = "s3.signer.endpoint";

  /** Boolean property that enables remote signing for tables. */
  public static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";

  public static final String AWS_REGION = "client.region";
  public static final String S3_ENDPOINT = "s3.endpoint";
  public static final String S3_ACCESS_POINTS_PREFIX = "s3.access-points.";
  public static final String S3_PATH_STYLE_ACCESS = "s3.path-style-access";
  private static final String S3_USE_ARN_REGION_ENABLED = "s3.use-arn-region-enabled";

  public static final String ICEBERG_PREFIX = "prefix";
  public static final String ICEBERG_URI = "uri";
  public static final String FILE_IO_IMPL = "io-impl";
  public static final String ICEBERG_WAREHOUSE_LOCATION = "warehouse";
  public static final String ICEBERG_TABLE_DEFAULT_PREFIX = "table-default.";
  public static final String ICEBERG_TABLE_OVERRIDE_PREFIX = "table-override.";

  private final NessieApiV2 nessieApi;
  private final CatalogConfig catalogConfig;
  private final RequestSigner signer;
  private final S3Options<?> s3Options;
  private final Optional<URI> tokenEndpoint;

  protected IcebergApiV1ResourceBase(
      CatalogService catalogService,
      ObjectIO objectIO,
      RequestSigner signer,
      NessieApiV2 nessieApi,
      CatalogConfig catalogConfig,
      S3Options<?> s3Options,
      Optional<URI> tokenEndpoint) {
    super(catalogService, objectIO);
    this.nessieApi = nessieApi;
    this.signer = signer;
    this.catalogConfig = catalogConfig;
    this.s3Options = s3Options;
    this.tokenEndpoint = tokenEndpoint;
  }

  public IcebergConfigResponse getConfig(String reference, String warehouse) {
    // TODO does it make sense to use a separate endpoint (service) just to expose configs,
    //   because those are accessed rather anonymously?

    WarehouseConfig w = catalogConfig.getWarehouse(warehouse);

    // TODO re-check the stuff that's returned from this function - there'll be a lot to be changed
    //  here, out of scope of the initial PR that adds Iceberg's REST endpoints.

    String branch = reference;
    if (branch == null) {
      branch = catalogConfig.defaultBranch().name();
    }
    if (branch == null) {
      branch = "main";
    }

    Map<String, String> configDefaults = new HashMap<>();
    Map<String, String> configOverrides = new HashMap<>();

    // Pass Nessie client properties to the client

    configDefaults.put(FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");

    configDefaults.putAll(catalogConfig.icebergConfigDefaults());
    configDefaults.putAll(w.icebergConfigDefaults());

    putS3ConfigOverrides(w, configOverrides);

    // Marker property telling clients that the backend is a Nessie Catalog.
    configOverrides.put("nessie.is-nessie-catalog", "true");

    // Make sure that `nessie.core-base-uri` always returns a `/` terminated URI.
    configOverrides.put("nessie.core-base-uri", uriInfo.coreRootURI().toString());
    // Make sure that `nessie.catalog-base-uri` always returns a `/` terminated URI.
    configOverrides.put("nessie.catalog-base-uri", uriInfo.catalogBaseURI().toString());
    // Iceberg base URI exposed twice for Spark SQL extensions, which update the `uri` config when
    // the branch is changed.
    configOverrides.put("nessie.iceberg-base-uri", uriInfo.icebergBaseURI().toString());

    configOverrides.put(ICEBERG_URI, uriInfo.icebergBaseURI().toString());

    // 'prefix-pattern' is just for information at the moment...
    configOverrides.put("nessie.prefix-pattern", "{ref}|{warehouse}");
    // The following properties are passed back to clients to automatically configure their Nessie
    // client. These properties are _not_ user configurable properties.
    configOverrides.put("nessie.default-branch.name", branch);
    // Set the "default" prefix
    configDefaults.put(ICEBERG_PREFIX, encode(branch, UTF_8));

    // FIXME the client needs this even before calling the /config endpoint
    tokenEndpoint.ifPresent(uri -> configOverrides.put("oauth2-server-uri", uri.toString()));
    // TODO scopes? But scopes are also needed before calling the /config endpoint

    configOverrides.putAll(catalogConfig.icebergConfigOverrides());
    configOverrides.putAll(w.icebergConfigOverrides());

    return IcebergConfigResponse.builder()
        .defaults(configDefaults)
        .overrides(configOverrides)
        .build();
  }

  private void putS3ConfigOverrides(
      WarehouseConfig warehouse, Map<String, String> configOverrides) {
    URI warehouseLocation = URI.create(warehouse.location());
    if (Objects.equals(warehouseLocation.getScheme(), "s3")) {
      // Warehouse location MUST be an s3 URI otherwise ResolvingFileIO will not work,
      // so we can safely assume the bucket is the whole authority. Cf. S3Utilities#parseUri.
      String bucket = warehouseLocation.getAuthority();
      S3BucketOptions s3BucketOptions =
          s3Options.effectiveOptionsForBucket(Optional.ofNullable(bucket));
      s3BucketOptions.region().ifPresent(r -> configOverrides.put(AWS_REGION, r));
      s3BucketOptions.endpoint().ifPresent(e -> configOverrides.put(S3_ENDPOINT, e.toString()));
      s3BucketOptions
          .accessPoint()
          .ifPresent(ap -> configOverrides.put(S3_ACCESS_POINTS_PREFIX + bucket, ap));
      s3BucketOptions
          .allowCrossRegionAccessPoint()
          .ifPresent(
              allow -> configOverrides.put(S3_USE_ARN_REGION_ENABLED, allow ? "true" : "false"));
      s3BucketOptions
          .pathStyleAccess()
          .ifPresent(psa -> configOverrides.put(S3_PATH_STYLE_ACCESS, psa ? "true" : "false"));
      configOverrides.put(S3_REMOTE_SIGNING_ENABLED, "true");
      // Note: leave S3 signer endpoint to its default for now, it is overridden on a per-table
      // basis, see loadTableResult() below
    }
  }

  //
  // S3 request signing
  //

  public IcebergS3SignResponse s3sign(
      IcebergS3SignRequest request, String prefix, String identifier) {
    ParsedReference ref = decodePrefix(prefix).parsedReference();

    URI uri = URI.create(request.uri());

    Optional<String> bucket = s3Options.extractBucket(uri);
    Optional<String> body = Optional.ofNullable(request.body());

    SigningRequest signingRequest =
        SigningRequest.signingRequest(
            uri, request.method(), request.region(), bucket, body, request.headers());

    SigningResponse signed = signer.sign(ref.name(), identifier, signingRequest);

    return icebergS3SignResponse(signed.uri().toString(), signed.headers());
  }

  //
  // OAuth proxy
  //

  public Response getToken() {
    return tokenEndpoint
        .map(Response::temporaryRedirect)
        .orElseGet(
            () ->
                Response.status(Status.SERVICE_UNAVAILABLE)
                    .entity(
                        Map.of(
                            "error", "OAuthTokenEndpointUnavailable",
                            "error_description", "OAuth token endpoint is unavailable")))
        .build();
  }

  //
  // Namespaces
  //

  public IcebergCreateNamespaceResponse createNamespace(
      String prefix, @Valid IcebergCreateNamespaceRequest createNamespaceRequest)
      throws IOException {
    ParsedReference ref = decodePrefix(prefix).parsedReference();

    Namespace ns =
        nessieApi
            .createNamespace()
            .refName(ref.name())
            .hashOnRef(ref.hashWithRelativeSpec())
            .namespace(createNamespaceRequest.namespace().toNessieNamespace())
            .properties(createNamespaceRequest.properties())
            .create();

    return IcebergCreateNamespaceResponse.builder()
        .namespace(createNamespaceRequest.namespace())
        .putAllProperties(ns.getProperties())
        .build();
  }

  public void dropNamespace(String prefix, String namespace) throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    nessieApi
        .deleteNamespace()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .delete();
  }

  public IcebergGetNamespaceResponse loadNamespaceMetadata(String prefix, String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    Namespace ns =
        nessieApi
            .getNamespace()
            .refName(namespaceRef.referenceName())
            .hashOnRef(namespaceRef.hashWithRelativeSpec())
            .namespace(namespaceRef.namespace())
            .get();

    return IcebergGetNamespaceResponse.builder()
        .namespace(IcebergNamespace.fromNessieNamespace(ns))
        .putAllProperties(ns.getProperties())
        .build();
  }

  public void namespaceExists(String prefix, String namespace) throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    nessieApi
        .getNamespace()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .get();
  }

  public IcebergListNamespacesResponse listNamespaces(
      String prefix, String parent, String pageToken, Integer pageSize) throws IOException {

    IcebergListNamespacesResponse.Builder response = IcebergListNamespacesResponse.builder();

    /*
    // Use the client-side get-multiple-namespaces here, it has all the needed CEL-expression logic
    nessieApi
        .getMultipleNamespaces()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .onlyDirectChildren(true)
        .get()
        .getNamespaces()
        .stream()
        .map(IcebergNamespace::fromNessieNamespace)
        .forEach(response::addNamespace);
     */

    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, parent);
    Namespace namespace = namespaceRef.namespace();
    String celFilter =
        "entry.contentType == 'NAMESPACE'"
            + ((namespace != null && !namespace.isEmpty())
                ? format(
                    " && size(entry.keyElements) == %d && entry.encodedKey.startsWith('%s.')",
                    namespace.getElementCount() + 1, namespace.name())
                : " && size(entry.keyElements) == 1");

    listContent(namespaceRef, pageToken, pageSize, true, celFilter, response::nextPageToken)
        .map(EntriesResponse.Entry::getContent)
        .map(Namespace.class::cast)
        .map(IcebergNamespace::fromNessieNamespace)
        .forEach(response::addNamespace);

    return response.build();
  }

  public IcebergUpdateNamespacePropertiesResponse updateProperties(
      String prefix,
      String namespace,
      @Valid IcebergUpdateNamespacePropertiesRequest updateNamespacePropertiesRequest)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    UpdateNamespaceResult namespaceUpdate =
        nessieApi
            .updateProperties()
            .refName(namespaceRef.referenceName())
            .hashOnRef(namespaceRef.hashWithRelativeSpec())
            .namespace(namespaceRef.namespace())
            .updateProperties(updateNamespacePropertiesRequest.updates())
            .removeProperties(new HashSet<>(updateNamespacePropertiesRequest.removals()))
            .updateWithResponse();

    IcebergUpdateNamespacePropertiesResponse.Builder response =
        IcebergUpdateNamespacePropertiesResponse.builder();

    Map<String, String> oldProperties = namespaceUpdate.getNamespaceBeforeUpdate().getProperties();
    Map<String, String> newProperties = namespaceUpdate.getNamespace().getProperties();

    oldProperties.keySet().stream()
        .filter(k -> !newProperties.containsKey(k))
        .forEach(response::addRemoved);

    Stream.concat(
            updateNamespacePropertiesRequest.removals().stream(),
            updateNamespacePropertiesRequest.updates().keySet().stream())
        .filter(k -> !oldProperties.containsKey(k))
        .forEach(response::addMissing);

    newProperties.entrySet().stream()
        .filter(
            e -> {
              String newValue = oldProperties.get(e.getKey());
              return !e.getValue().equals(newValue);
            })
        .map(Map.Entry::getKey)
        .forEach(response::addUpdated);

    return response.build();
  }

  //
  // Tables
  //

  public Uni<IcebergCreateTableResponse> createTable(
      String prefix,
      String namespace,
      @Valid IcebergCreateTableRequest createTableRequest,
      String dataAccess)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, createTableRequest.name());
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    IcebergSortOrder sortOrder = createTableRequest.writeOrder();
    if (sortOrder == null) {
      sortOrder = unsorted();
    }
    IcebergPartitionSpec spec = createTableRequest.partitionSpec();
    if (spec == null) {
      spec = unpartitioned();
    }
    IcebergSchema schema = createTableRequest.schema();
    String location = createTableRequest.location();
    if (location == null) {
      location = defaultTableLocation(createTableRequest.location(), tableRef);
    }
    Map<String, String> properties = new HashMap<>();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(createTableRequest.properties());

    List<IcebergMetadataUpdate> updates =
        Arrays.asList(
            assignUUID(randomUUID().toString()),
            upgradeFormatVersion(2),
            addSchema(schema, 0),
            setCurrentSchema(-1),
            addPartitionSpec(spec),
            setDefaultPartitionSpec(-1),
            addSortOrder(sortOrder),
            setDefaultSortOrder(-1),
            setLocation(location),
            setProperties(properties));

    GetMultipleContentsResponse contentResponse =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .key(tableRef.contentKey())
            .getWithResponse();
    if (!contentResponse.getContents().isEmpty()) {
      Content existing = contentResponse.getContents().get(0).getContent();
      throw new IcebergConflictException(
          "AlreadyExistsException",
          format(
              "%s %salready exists: %s",
              typeToEntityName(existing.getType()),
              ICEBERG_TABLE.equals(existing.getType()) ? "" : "with same name ",
              tableRef.contentKey()));
    }
    Branch target = checkBranch(contentResponse.getEffectiveReference());

    if (createTableRequest.stageCreate()) {
      // TODO reduce log level to trace (or remove logging)
      LOGGER.info(
          "Staged create-table for table '{}' on '{}' with schema: {}, spec: {}, sort-order: {}",
          tableRef.contentKey(),
          target,
          schema,
          spec,
          sortOrder);

      NessieTableSnapshot snapshot =
          new IcebergTableMetadataUpdateState(
                  newIcebergTableSnapshot(updates), tableRef.contentKey(), false)
              .applyUpdates(updates)
              .snapshot();

      IcebergTableMetadata stagedTableMetadata =
          nessieTableSnapshotToIceberg(
              snapshot, Optional.empty(), NessieModelIceberg.IcebergSnapshotTweak.NOOP, map -> {});

      return Uni.createFrom()
          .item(
              loadTableResult(
                  null,
                  stagedTableMetadata,
                  IcebergCreateTableResponse.builder(),
                  prefix,
                  tableRef.contentKey()));
    }
    LOGGER.info("Create-table for table '{}' on '{}'", tableRef.contentKey(), target.getName());

    IcebergUpdateTableRequest updateTableReq =
        IcebergUpdateTableRequest.builder()
            .identifier(fromNessieContentKey(tableRef.contentKey()))
            .addAllUpdates(updates)
            .addRequirement(IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist())
            .build();

    return createOrUpdateTable(tableRef, warehouse, updateTableReq)
        .map(
            snap ->
                loadTableResultFromSnapshotResponse(
                    snap, IcebergCreateTableResponse.builder(), prefix, tableRef.contentKey()));
  }

  public void dropTable(String prefix, String namespace, String table, Boolean purgeRequested)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    ContentResponse resp = fetchIcebergTable(tableRef);
    Branch ref = checkBranch(resp.getEffectiveReference());

    nessieApi
        .commitMultipleOperations()
        .branch(ref)
        .commitMeta(fromMessage(format("drop table %s", tableRef.contentKey())))
        .operation(Operation.Delete.of(tableRef.contentKey()))
        .commitWithResponse();
  }

  public Uni<IcebergLoadTableResponse> registerTable(
      String prefix, String namespace, @Valid IcebergRegisterTableRequest registerTableRequest)
      throws IOException, IcebergConflictException {

    TableRef tableRef = decodeTableRef(prefix, namespace, registerTableRequest.name());
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    try {
      fetchIcebergTable(tableRef);
      throw new IcebergConflictException(
          "AlreadyExistsException", "Table already exists: " + tableRef.contentKey());
    } catch (NessieContentNotFoundException e) {
      // this is what we want
    }

    Branch ref = checkBranch(nessieApi.getReference().refName(tableRef.reference().name()).get());

    Optional<TableRef> catalogTableRef =
        uriInfo.resolveTableFromUri(registerTableRequest.metadataLocation());
    boolean nessieCatalogUri = uriInfo.isNessieCatalogUri(registerTableRequest.metadataLocation());
    if (catalogTableRef.isPresent() && nessieCatalogUri) {
      // In case the metadataLocation in the IcebergRegisterTableRequest contains a URI for _this_
      // Nessie Catalog, use the existing data/objects.

      // Taking a "shortcut" here, we use the 'old Content object' and re-add it in a Nessie commit.

      TableRef ctr = catalogTableRef.get();

      SnapshotReqParams reqParams =
          SnapshotReqParams.forSnapshotHttpReq(ctr.reference(), "iceberg_imported", null);

      CatalogService.CatalogUriResolver catalogUriResolver =
          new CatalogUriResolverImpl(uriInfo, reqParams.snapshotFormat());

      ContentResponse contentResponse = fetchIcebergTable(ctr);
      // It's technically a new table for Nessie, so need to clear the content-ID.
      Content newContent = contentResponse.getContent().withId(null);

      CommitResponse committed =
          nessieApi
              .commitMultipleOperations()
              .branch(ref)
              .commitMeta(
                  fromMessage(
                      format(
                          "Register Iceberg table '%s' from '%s'",
                          ctr.contentKey(), registerTableRequest.metadataLocation())))
              .operation(Operation.Put.of(ctr.contentKey(), newContent))
              .commitWithResponse();

      return loadTable(
          TableRef.tableRef(
              ctr.contentKey(),
              ParsedReference.parsedReference(
                  committed.getTargetBranch().getName(),
                  committed.getTargetBranch().getHash(),
                  Reference.ReferenceType.BRANCH),
              null),
          prefix);
    } else if (nessieCatalogUri) {
      throw new IllegalArgumentException(
          "Cannot register an Iceberg table using the URI "
              + registerTableRequest.metadataLocation());
    }

    // Register table from "external" metadata-location

    // TODO We need the table-metadata to import here to construct the Nessie `IcebergTable` content
    //  object. But the `loadTable()` below will trigger another read of the same table-metadata
    //  object. Need a way to prevent the duplicate read operation.

    IcebergTableMetadata tableMetadata;
    try (InputStream metadataInput =
        objectIO.readObject(URI.create(registerTableRequest.metadataLocation()))) {
      tableMetadata =
          IcebergJson.objectMapper().readValue(metadataInput, IcebergTableMetadata.class);
    }

    ToIntFunction<Integer> safeUnbox = i -> i != null ? i : 0;

    Content newContent =
        IcebergTable.of(
            registerTableRequest.metadataLocation(),
            tableMetadata.currentSnapshotId(),
            safeUnbox.applyAsInt(tableMetadata.currentSchemaId()),
            safeUnbox.applyAsInt(tableMetadata.defaultSpecId()),
            safeUnbox.applyAsInt(tableMetadata.defaultSortOrderId()));
    CommitResponse committed =
        nessieApi
            .commitMultipleOperations()
            .branch(ref)
            .commitMeta(
                fromMessage(
                    format(
                        "Register Iceberg table '%s' from '%s'",
                        tableRef.contentKey(), registerTableRequest.metadataLocation())))
            .operation(Operation.Put.of(tableRef.contentKey(), newContent))
            .commitWithResponse();

    return loadTable(
        tableRef(
            tableRef.contentKey(),
            parsedReference(
                committed.getTargetBranch().getName(),
                committed.getTargetBranch().getHash(),
                committed.getTargetBranch().getType()),
            null),
        prefix);
  }

  public IcebergListTablesResponse listTables(
      String prefix, String namespace, String pageToken, Integer pageSize) throws IOException {
    IcebergListTablesResponse.Builder response = IcebergListTablesResponse.builder();

    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);
    String celFilter =
        format(
            "entry.contentType == 'ICEBERG_TABLE' && entry.encodedKey.startsWith('%s.')",
            namespaceRef.namespace().toPathString());

    listContent(namespaceRef, pageToken, pageSize, false, celFilter, response::nextPageToken)
        .map(e -> fromNessieContentKey(e.getName()))
        .forEach(response::addIdentifier);

    return response.build();
  }

  public Uni<IcebergLoadTableResponse> loadTable(
      String prefix, String namespace, String table, String snapshots, String dataAccess)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    return loadTable(tableRef, prefix);
  }

  public void tableExists(String prefix, String namespace, String table) throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    fetchIcebergTable(tableRef);
  }

  public Uni<IcebergCommitTableResponse> updateTable(
      String prefix,
      String namespace,
      String table,
      @Valid IcebergUpdateTableRequest commitTableRequest)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, table);
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    return createOrUpdateTable(tableRef, warehouse, commitTableRequest)
        .map(
            snap -> {
              IcebergTableMetadata tableMetadata =
                  (IcebergTableMetadata) snap.entityObject().orElseThrow();
              return IcebergCommitTableResponse.builder()
                  .metadata(tableMetadata)
                  .metadataLocation(snapshotMetadataLocation(snap))
                  // TODO there is no 'config' in IcebergCommitTableResponse, so no place to change
                  //  vended authorization tokens for file/object access
                  .build();
            });
  }

  public void renameTable(
      String prefix, @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IOException, IcebergConflictException {

    renameContent(prefix, renameTableRequest, ICEBERG_TABLE);
  }

  //
  // Views
  //

  public Uni<IcebergLoadViewResponse> createView(
      String prefix, String namespace, @Valid IcebergCreateViewRequest createViewRequest)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, createViewRequest.name());
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    createViewRequest.viewVersion();

    IcebergSchema schema = createViewRequest.schema();
    String location = createViewRequest.location();
    if (location == null) {
      location = defaultTableLocation(createViewRequest.location(), tableRef);
    }
    Map<String, String> properties = new HashMap<>();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(createViewRequest.properties());

    List<IcebergMetadataUpdate> updates =
        Arrays.asList(
            assignUUID(randomUUID().toString()),
            upgradeFormatVersion(1),
            addSchema(schema, 0),
            setCurrentSchema(-1),
            setLocation(location),
            setProperties(properties),
            addViewVersion(createViewRequest.viewVersion()),
            setCurrentViewVersion(-1L));

    GetMultipleContentsResponse contentResponse =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .key(tableRef.contentKey())
            .getWithResponse();
    if (!contentResponse.getContents().isEmpty()) {
      Content existing = contentResponse.getContents().get(0).getContent();
      throw new IcebergConflictException(
          "CommitFailedException",
          format(
              "%s %salready exists: %s",
              typeToEntityName(existing.getType()),
              ICEBERG_VIEW.equals(existing.getType()) ? "" : "with same name ",
              tableRef.contentKey()));
    }
    Branch target = checkBranch(contentResponse.getEffectiveReference());

    LOGGER.info("Create-view for view '{}' on '{}'", tableRef.contentKey(), target.getName());

    IcebergCommitViewRequest updateTableReq =
        IcebergCommitViewRequest.builder()
            .identifier(fromNessieContentKey(tableRef.contentKey()))
            .addAllUpdates(updates)
            .addRequirement(IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist())
            .build();

    return createOrUpdateView(tableRef, warehouse, updateTableReq)
        .map(snap -> loadViewResultFromSnapshotResponse(snap, IcebergLoadViewResponse.builder()));
  }

  public void dropView(String prefix, String namespace, String view, Boolean purgeRequested)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, view);

    ContentResponse resp = fetchIcebergView(tableRef);
    Branch ref = checkBranch(resp.getEffectiveReference());

    nessieApi
        .commitMultipleOperations()
        .branch(ref)
        .commitMeta(fromMessage(format("drop view %s", tableRef.contentKey())))
        .operation(Operation.Delete.of(tableRef.contentKey()))
        .commitWithResponse();
  }

  public IcebergListTablesResponse listViews(
      String prefix, String namespace, String pageToken, Integer pageSize) throws IOException {

    IcebergListTablesResponse.Builder response = IcebergListTablesResponse.builder();

    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);
    String celFilter =
        format(
            "entry.contentType == 'ICEBERG_VIEW' && entry.encodedKey.startsWith('%s.')",
            namespaceRef.namespace().toPathString());

    listContent(namespaceRef, pageToken, pageSize, false, celFilter, response::nextPageToken)
        .map(e -> fromNessieContentKey(e.getName()))
        .forEach(response::addIdentifier);

    return response.build();
  }

  public Uni<IcebergLoadViewResponse> loadView(String prefix, String namespace, String table)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    return loadView(tableRef);
  }

  public void viewExists(String prefix, String namespace, String view) throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, view);

    fetchIcebergView(tableRef);
  }

  public Uni<IcebergLoadViewResponse> updateView(
      String prefix,
      String namespace,
      String view,
      @Valid IcebergCommitViewRequest commitViewRequest)
      throws IOException, IcebergConflictException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, view);
    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());

    return createOrUpdateView(tableRef, warehouse, commitViewRequest)
        .map(
            snap -> {
              IcebergViewMetadata viewMetadata =
                  (IcebergViewMetadata) snap.entityObject().orElseThrow();
              return IcebergLoadViewResponse.builder()
                  .metadata(viewMetadata)
                  .metadataLocation(snapshotMetadataLocation(snap))
                  // TODO there is no 'config' in IcebergCommitTableResponse, so no place to change
                  //  vended authorization tokens for file/object access
                  .build();
            });
  }

  public void renameView(
      String prefix, @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IOException, IcebergConflictException {

    renameContent(prefix, renameTableRequest, ICEBERG_VIEW);
  }

  //
  // Multi table transactions
  //

  public Uni<Void> commitTransaction(
      String prefix, @Valid IcebergCommitTransactionRequest commitTransactionRequest)
      throws IOException, IcebergConflictException {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();

    WarehouseConfig warehouse = catalogConfig.getWarehouse(decoded.warehouse());

    CommitMeta commitMeta = fromMessage("foo");

    CatalogCommit.Builder commit = CatalogCommit.builder().commitMeta(commitMeta);
    commitTransactionRequest.tableChanges().stream()
        .map(
            tableChange -> {
              ContentKey key = requireNonNull(tableChange.identifier()).toNessieContentKey();

              return IcebergCatalogOperation.builder()
                  .updates(tableChange.updates())
                  .requirements(tableChange.requirements())
                  .key(key)
                  .type(ICEBERG_TABLE)
                  .build();
            })
        .forEach(commit::addOperations);

    SnapshotReqParams reqParams =
        SnapshotReqParams.forSnapshotHttpReq(ref, "iceberg_imported", null);

    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo, reqParams.snapshotFormat());

    // Although we don't return anything, need to make sure that the commit operation starts and all
    // results are consumed.
    return Uni.createFrom()
        .completionStage(catalogService.commit(ref, commit.build(), reqParams, catalogUriResolver))
        .map(stream -> stream.reduce(null, (ident, snap) -> ident, (i1, i2) -> i1));
  }

  //
  // Metrics
  //

  public void reportMetrics(
      String prefix,
      String namespace,
      String table,
      @Valid @NotNull IcebergMetricsReport reportMetricsRequest)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    // Using the effective reference from ContentResponse would be wrong here, because we do not
    // know the commit ID for/on which the metrics were generated, unless the hash is included in
    // TableRef.

    pushMetrics(tableRef, reportMetricsRequest);
  }

  //
  // Private / common code
  //

  private Stream<EntriesResponse.Entry> listContent(
      NamespaceRef namespaceRef,
      String pageToken,
      Integer pageSize,
      boolean withContent,
      String celFilter,
      Consumer<String> responsePagingToken)
      throws NessieNotFoundException {

    // TODO marry Nessie paging with Iceberg paging

    EntriesResponse entriesResponse =
        applyPaging(
                nessieApi
                    .getEntries()
                    .refName(namespaceRef.referenceName())
                    .hashOnRef(namespaceRef.hashWithRelativeSpec())
                    .filter(celFilter)
                    .withContent(withContent),
                pageToken,
                pageSize)
            .get();

    String token = entriesResponse.getToken();
    if (token != null) {
      responsePagingToken.accept(token);
    }

    return entriesResponse.getEntries().stream();
  }

  private static <P extends PagingBuilder<?, ?, ?>> P applyPaging(
      P pageable, String pageToken, Integer pageSize) {
    if (pageSize != null) {
      if (pageToken != null) {
        pageable.pageToken(pageToken);
      }
      pageable.maxRecords(pageSize);
    }

    return pageable;
  }

  private Uni<IcebergLoadViewResponse> loadView(TableRef tableRef) throws NessieNotFoundException {
    ContentKey key = tableRef.contentKey();

    return snapshotResponse(
            key,
            SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg_imported", null),
            ICEBERG_VIEW)
        .map(snap -> loadViewResultFromSnapshotResponse(snap, IcebergLoadViewResponse.builder()));
  }

  private Uni<IcebergLoadTableResponse> loadTable(TableRef tableRef, String prefix)
      throws NessieNotFoundException {
    ContentKey key = tableRef.contentKey();

    return snapshotResponse(
            key,
            SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg_imported", null),
            ICEBERG_TABLE)
        .map(
            snap ->
                loadTableResultFromSnapshotResponse(
                    snap, IcebergLoadTableResponse.builder(), prefix, key));
  }

  private void renameContent(
      String prefix, IcebergRenameTableRequest renameTableRequest, Content.Type expectedContentType)
      throws NessieNotFoundException, IcebergConflictException, NessieConflictException {
    TableRef fromTableRef = decodeTableRef(prefix, renameTableRequest.source());
    TableRef toTableRef = decodeTableRef(prefix, renameTableRequest.destination());

    GetMultipleContentsResponse contents =
        nessieApi
            .getContent()
            .refName(fromTableRef.reference().name())
            .hashOnRef(fromTableRef.reference().hashWithRelativeSpec())
            .key(toTableRef.contentKey())
            .key(fromTableRef.contentKey())
            .getWithResponse();
    Map<ContentKey, Content> contentsMap = contents.toContentsMap();
    Content existingFrom = contentsMap.get(fromTableRef.contentKey());
    if (existingFrom == null || !expectedContentType.equals(existingFrom.getType())) {
      throw new NessieContentNotFoundException(
          fromTableRef.contentKey(), renameTableRequest.source().name());
    }

    Reference effectiveRef = contents.getEffectiveReference();

    Content existingTo = contentsMap.get(toTableRef.contentKey());
    if (existingTo != null) {
      String existingEntityName = typeToEntityName(existingTo.getType());
      // TODO throw ViewAlreadyExistsError ?
      // TODO throw TableAlreadyExistsError ?
      throw new IcebergConflictException(
          "AlreadyExistsException",
          format(
              "Cannot rename %s to %s. %s already exists",
              fromTableRef.contentKey(), toTableRef.contentKey(), existingEntityName));
    }

    String entityType = typeToEntityName(expectedContentType).toLowerCase(Locale.ROOT);
    checkArgument(
        effectiveRef instanceof Branch,
        format("Must only rename a %s on a branch, but target is %s", entityType, effectiveRef));

    nessieApi
        .commitMultipleOperations()
        .branch((Branch) effectiveRef)
        .commitMeta(
            fromMessage(
                format(
                    "rename %s %s to %s",
                    entityType, fromTableRef.contentKey(), toTableRef.contentKey())))
        .operation(Operation.Delete.of(fromTableRef.contentKey()))
        .operation(Operation.Put.of(toTableRef.contentKey(), existingFrom))
        .commitWithResponse();
  }

  Uni<SnapshotResponse> createOrUpdateTable(
      TableRef tableRef, WarehouseConfig warehouse, IcebergUpdateTableRequest commitTableRequest)
      throws IOException {

    boolean isCreate =
        commitTableRequest.requirements().stream()
            .anyMatch(IcebergUpdateRequirement.AssertCreate.class::isInstance);
    if (isCreate) {
      List<IcebergUpdateRequirement> invalidRequirements =
          commitTableRequest.requirements().stream()
              .filter(req -> !(req instanceof IcebergUpdateRequirement.AssertCreate))
              .collect(Collectors.toList());
      checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    // TODO commit message
    CommitMeta commitMeta = fromMessage("foo");
    IcebergCatalogOperation op =
        IcebergCatalogOperation.builder()
            .updates(commitTableRequest.updates())
            .requirements(commitTableRequest.requirements())
            .key(tableRef.contentKey())
            .type(ICEBERG_TABLE)
            .build();

    CatalogCommit commit = CatalogCommit.builder().commitMeta(commitMeta).addOperations(op).build();

    SnapshotReqParams reqParams =
        SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg_imported", null);

    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo, reqParams.snapshotFormat());

    return Uni.createFrom()
        .completionStage(
            catalogService.commit(tableRef.reference(), commit, reqParams, catalogUriResolver))
        .map(Stream::findFirst)
        .map(Optional::orElseThrow);
  }

  Uni<SnapshotResponse> createOrUpdateView(
      TableRef tableRef, WarehouseConfig warehouse, IcebergCommitViewRequest commitViewRequest)
      throws IOException {

    boolean isCreate =
        commitViewRequest.requirements().stream()
            .anyMatch(IcebergUpdateRequirement.AssertCreate.class::isInstance);
    if (isCreate) {
      List<IcebergUpdateRequirement> invalidRequirements =
          commitViewRequest.requirements().stream()
              .filter(req -> !(req instanceof IcebergUpdateRequirement.AssertCreate))
              .collect(Collectors.toList());
      checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    // TODO commit message
    CommitMeta commitMeta = fromMessage("foo");
    IcebergCatalogOperation op =
        IcebergCatalogOperation.builder()
            .updates(commitViewRequest.updates())
            .requirements(commitViewRequest.requirements())
            .key(tableRef.contentKey())
            .type(ICEBERG_VIEW)
            .build();

    CatalogCommit commit = CatalogCommit.builder().commitMeta(commitMeta).addOperations(op).build();

    SnapshotReqParams reqParams =
        SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg_imported", null);

    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo, reqParams.snapshotFormat());

    return Uni.createFrom()
        .completionStage(
            catalogService.commit(tableRef.reference(), commit, reqParams, catalogUriResolver))
        .map(Stream::findFirst)
        .map(Optional::orElseThrow);
  }

  private void pushMetrics(TableRef tableRef, IcebergMetricsReport report) {
    // TODO push metrics to "somewhere".
    // TODO note that metrics for "staged tables" are also received, even if those do not yet exist
  }

  protected NamespaceRef decodeNamespaceRef(String prefix, String encodedNs) {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();
    Namespace ns = decodeNamespace(encodedNs);
    return namespaceRef(ns, ref.name(), ref.hashWithRelativeSpec(), decoded.warehouse());
  }

  protected DecodedPrefix decodePrefix(String prefix) {
    ParsedReference parsedReference = catalogConfig.defaultBranch();
    String warehouse = catalogConfig.defaultWarehouse().name();

    return decodePrefix(
        prefix, parsedReference, warehouse, () -> catalogConfig.defaultBranch().name());
  }

  public TableRef decodeTableRefWithHash(String prefix, String encodedNs, String table)
      throws NessieNotFoundException {
    TableRef tableRef = decodeTableRef(prefix, encodedNs, table);

    ParsedReference reference = tableRef.reference();
    if (reference.hashWithRelativeSpec() == null) {
      Reference ref = nessieApi.getReference().refName(reference.name()).get();
      reference = ParsedReference.parsedReference(ref.getName(), ref.getHash(), ref.getType());
      return tableRef(tableRef.contentKey(), reference, tableRef.warehouse());
    }

    return tableRef;
  }

  public TableRef decodeTableRef(String prefix, String encodedNs, String table) {
    Namespace ns = decodeNamespace(encodedNs);
    TableReference tableReference = TableReference.parse(table);

    return fixupTableRef(prefix, tableReference, ns);
  }

  public TableRef decodeTableRef(String prefix, IcebergTableIdentifier table) {
    TableReference tableReference = TableReference.parse(table.name());
    Namespace ns = Namespace.of(table.namespace().levels());

    return fixupTableRef(prefix, tableReference, ns);
  }

  private TableRef fixupTableRef(String prefix, TableReference tableReference, Namespace ns) {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();
    ContentKey contentKey = ContentKey.of(ns, tableReference.getName());
    String refName =
        tableReference.getReference() != null ? tableReference.getReference() : ref.name();
    String refHash =
        tableReference.getHash() != null ? tableReference.getHash() : ref.hashWithRelativeSpec();
    return tableRef(contentKey, parsedReference(refName, refHash, null), decoded.warehouse());
  }

  public static Namespace decodeNamespace(String encodedNs) {
    if (encodedNs == null) {
      return Namespace.EMPTY;
    }

    return Namespace.of(NAMESPACE_ESCAPED_SPLITTER.splitToList(encodedNs).toArray(new String[0]));
  }

  public static final char SEPARATOR = '\u001f';
  private static final String DEFAULT_REF_IN_PATH = "-";
  private static final Splitter NAMESPACE_ESCAPED_SPLITTER = Splitter.on(SEPARATOR);

  @VisibleForTesting
  static DecodedPrefix decodePrefix(
      String prefix,
      ParsedReference parsedReference,
      String warehouse,
      Supplier<String> defaultBranchSupplier) {
    if (prefix != null) {
      prefix = prefix.replace(SEPARATOR, '/');

      int indexAt = prefix.indexOf('|');
      if (indexAt != -1) {
        if (indexAt != prefix.length() - 1) {
          warehouse = prefix.substring(indexAt + 1);
        }
        prefix = prefix.substring(0, indexAt);
      }

      if (!prefix.isEmpty() && !DEFAULT_REF_IN_PATH.equals(prefix)) {
        parsedReference = resolveReferencePathElement(prefix, null, defaultBranchSupplier);
      }
    }

    return decodedPrefix(parsedReference, warehouse);
  }

  static Branch checkBranch(Reference reference) {
    checkArgument(
        reference instanceof Branch, "Can only commit against a branch, but got " + reference);
    return (Branch) reference;
  }

  private String defaultTableLocation(String location, TableRef tableRef) {
    if (location != null) {
      return location;
    }

    WarehouseConfig warehouse = catalogConfig.getWarehouse(tableRef.warehouse());
    ContentKey key = tableRef.contentKey();

    // TODO do we need to URLEncode the path elements of the Namespace and ContentKey ?

    Namespace ns = key.getNamespace();
    if (!ns.isEmpty()) {
      List<ContentKey> parentNamespaces = new ArrayList<>();
      for (Namespace namespace = key.getNamespace();
          !namespace.isEmpty();
          namespace = namespace.getParentOrEmpty()) {
        parentNamespaces.add(namespace.toContentKey());
      }

      String baseLocation = concatLocation(warehouse.location(), ns.toString());
      try {
        ParsedReference parsedRef = requireNonNull(tableRef.reference());
        GetMultipleContentsResponse namespacesResp =
            nessieApi
                .getContent()
                .reference(Branch.of(parsedRef.name(), parsedRef.hashWithRelativeSpec()))
                .keys(parentNamespaces)
                .getWithResponse();
        Map<ContentKey, Content> namespacesMap = namespacesResp.toContentsMap();
        for (ContentKey nsKey : parentNamespaces) {
          Content namespace = namespacesMap.get(nsKey);
          if (namespace instanceof Namespace) {
            String namespaceLocation = ((Namespace) namespace).getProperties().get("location");
            if (namespaceLocation != null) {
              baseLocation = namespaceLocation;
              break;
            }
          }
        }
      } catch (NessieNamespaceNotFoundException e) {
        // ignore
      } catch (NessieNotFoundException e) {
        // do nothing we want the same behavior that if the location is not defined
      }

      location = concatLocation(baseLocation, key.getName());
    } else {
      location = concatLocation(warehouse.location(), key.getName());
    }
    // Different tables with same table name can exist across references in Nessie.
    // To avoid sharing same table path between two tables with same name, use uuid in the table
    // path.
    return location + "_" + UUID.randomUUID();
  }

  private String concatLocation(String location, String key) {
    if (location.endsWith("/")) {
      return location + key;
    }
    return location + "/" + key;
  }

  private ContentResponse fetchIcebergTable(TableRef tableRef) throws NessieNotFoundException {
    ContentResponse content =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .getSingle(tableRef.contentKey());
    checkArgument(
        content.getContent().getType().equals(ICEBERG_TABLE),
        "Table is not an Iceberg table, it is of type %s",
        content.getContent().getType());
    return content;
  }

  private ContentResponse fetchIcebergView(TableRef tableRef) throws NessieNotFoundException {
    ContentResponse content =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .getSingle(tableRef.contentKey());
    checkArgument(
        content.getContent().getType().equals(ICEBERG_VIEW),
        "View is not an Iceberg view, it is of type %s",
        content.getContent().getType());
    return content;
  }

  private String snapshotMetadataLocation(SnapshotResponse snap) {
    // TODO the resolved metadataLocation is wrong !!
    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo, SnapshotFormat.ICEBERG_TABLE_METADATA_IMPORTED);
    URI metadataLocation =
        catalogUriResolver.icebergSnapshot(
            snap.effectiveReference(), snap.contentKey(), snap.nessieSnapshot());
    return metadataLocation.toString();
  }

  private <R extends IcebergLoadTableResult, B extends IcebergLoadTableResult.Builder<R, B>>
      R loadTableResult(
          String metadataLocation,
          IcebergTableMetadata tableMetadata,
          B builder,
          String prefix,
          ContentKey contentKey) {
    return builder
        .metadata(tableMetadata)
        .metadataLocation(metadataLocation)
        // TODO this is the place to add vended authorization tokens for file/object access
        .putConfig(
            S3_SIGNER_ENDPOINT,
            // TODO does it make sense to use a separate endpoint (service) just for signing?
            uriInfo
                .icebergBaseURI()
                .resolve(
                    format(
                        "v1/%s/s3-sign/%s",
                        encode(prefix, UTF_8), encode(contentKey.toPathString(), UTF_8)))
                .toString())
        .build();
  }

  private IcebergLoadViewResponse loadViewResult(
      String metadataLocation,
      IcebergViewMetadata viewMetadata,
      IcebergLoadViewResponse.Builder builder) {
    return builder
        .metadata(viewMetadata)
        .metadataLocation(metadataLocation)
        // TODO this is the place to add vended authorization tokens for file/object access
        // .config(...)
        .build();
  }

  private <R extends IcebergLoadTableResult, B extends IcebergLoadTableResult.Builder<R, B>>
      R loadTableResultFromSnapshotResponse(
          SnapshotResponse snap, B builder, String prefix, ContentKey contentKey) {
    IcebergTableMetadata tableMetadata = (IcebergTableMetadata) snap.entityObject().orElseThrow();
    return loadTableResult(
        snapshotMetadataLocation(snap), tableMetadata, builder, prefix, contentKey);
  }

  private IcebergLoadViewResponse loadViewResultFromSnapshotResponse(
      SnapshotResponse snap, IcebergLoadViewResponse.Builder builder) {
    IcebergViewMetadata viewMetadata = (IcebergViewMetadata) snap.entityObject().orElseThrow();
    return loadViewResult(snapshotMetadataLocation(snap), viewMetadata, builder);
  }
}
