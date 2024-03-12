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
package org.apache.iceberg.nessie;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.LocationProvider;
import org.projectnessie.catalog.api.base.transport.ImmutableCatalogCommit;
import org.projectnessie.catalog.iceberg.httpfileio.HttpFileIO;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Constructor of NessieTableOperations is package-private - therefore this class is in the
//  org.apache.iceberg.nessie package
public class NessieCatalogTableOperations extends NessieTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(NessieCatalogTableOperations.class);

  private final HttpClient httpClient;

  // TODO 'client' field is private in NessieCatalog
  private final NessieIcebergClient client;
  // TODO 'key' field is private in NessieCatalog
  private final ContentKey key;
  private final NessieContentAwareFileIO contentAwareFileIO;
  private final URI baseUri;
  private final boolean sendUpdatesToServer;

  public NessieCatalogTableOperations(
      ContentKey key,
      NessieIcebergClient client,
      NessieContentAwareFileIO fileIO,
      Map<String, String> catalogOptions,
      boolean sendUpdatesToServer) {
    super(key, client, new RedirectingFileIO(new HttpFileIO(fileIO), key, client), catalogOptions);

    this.client = client;
    this.key = key;
    this.contentAwareFileIO = fileIO;
    this.sendUpdatesToServer = sendUpdatesToServer;

    NessieApiV1 api = client.getApi();
    this.httpClient =
        api.unwrapClient(HttpClient.class)
            .orElseThrow(() -> new IllegalArgumentException("Nessie client must use HTTP"));
    this.baseUri = resolveCatalogBaseUri();
  }

  private URI resolveCatalogBaseUri() {
    URI coreBaseUri = httpClient.getBaseUri();
    return coreBaseUri.resolve(
        coreBaseUri.getPath().endsWith("/") ? "../../catalog/v1/" : "../catalog/v1/");
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    if (!sendUpdatesToServer) {
      super.doCommit(base, metadata);
      return;
    }

    ImmutableIcebergCatalogOperation.Builder op =
        ImmutableIcebergCatalogOperation.builder().key(key).type(Content.Type.ICEBERG_TABLE);

    op.addAllUpdates(metadata.changes());

    if (base == null) { // new table
      op.addUpdate(new MetadataUpdate.AssignUUID(metadata.uuid()));
      op.addUpdate(new MetadataUpdate.UpgradeFormatVersion(metadata.formatVersion()));
      op.addUpdate(new MetadataUpdate.SetCurrentSchema(-1));
      op.addUpdate(new MetadataUpdate.SetDefaultPartitionSpec(-1));
      op.addUpdate(new MetadataUpdate.SetDefaultSortOrder(-1));
      // TODO re-check the arg of forCreateTable()
      op.addAllRequirements(UpdateRequirements.forCreateTable(Collections.emptyList()));
    } else {
      op.addAllRequirements(UpdateRequirements.forUpdateTable(base, metadata.changes()));
    }

    // TODO: commit message
    ImmutableCatalogCommit.Builder commit =
        ImmutableCatalogCommit.builder()
            .commitMeta(CommitMeta.fromMessage("Iceberg commit"))
            .addOperations(op.build());

    String refName = client.refName();

    try {
      httpClient
          .newRequest(baseUri)
          .path("trees/{ref}/commit")
          .resolveTemplate(
              "ref", client.getReference().toPathString()) // TODO: commit hash from meta
          .unwrap(NessieNotFoundException.class, NessieConflictException.class)
          .post(commit.build());
    } catch (NessieConflictException ex) {
      if (ex instanceof NessieReferenceConflictException) {
        // Throws a specialized exception, if possible
        maybeThrowSpecializedException((NessieReferenceConflictException) ex);
      }
      throw new CommitFailedException(
          ex,
          "Cannot commit: Reference hash is out of date. "
              + "Update the reference '%s' and try again",
          refName);
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(
          String.format("Cannot commit: Reference '%s' no longer exists", refName), ex);
    }
  }

  @Override
  public String metadataFileLocation(String filename) {
    String location = super.metadataFileLocation(filename);
    LOG.info("New metadata file {} mapped to {}", filename, location);
    return location;
  }

  @Override
  public LocationProvider locationProvider() {
    LOG.info("locationProvider() called");
    return new LoggingLocationProvider(super.locationProvider());
  }

  // LocationProvider is being serialized :facepalm:
  static final class LoggingLocationProvider implements LocationProvider {
    private final LocationProvider delegate;

    LoggingLocationProvider(LocationProvider delegate) {
      this.delegate = delegate;
    }

    @Override
    public String newDataLocation(String filename) {
      String location = delegate.newDataLocation(filename);
      LOG.info("New data file {} mapped to {}", filename, location);
      return location;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      String location = delegate.newDataLocation(filename);
      LOG.info("New data file {} for partition {} mapped to {}", filename, partitionData, location);
      return location;
    }
  }

  @Override
  protected void doRefresh() {
    try {
      client.refresh();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Failed to refresh as ref '%s' is no longer valid.", client.getRef().getName()),
          e);
    }

    Reference reference = client.getRef().getReference();
    contentAwareFileIO.setReference(reference.toPathString());
    try {
      HttpResponse response =
          httpClient
              .newRequest(baseUri)
              .path("trees/{ref}/snapshot/{key}")
              .resolveTemplate("ref", reference.toPathString())
              .resolveTemplate("key", key.toPathString())
              .queryParam("format", "iceberg_imported")
              .unwrap(NessieNotFoundException.class)
              .get();
      JsonNode tableMetadata = response.readEntity(JsonNode.class);

      TableMetadata icebergMetadata = TableMetadataParser.fromJson(tableMetadata);

      // TODO: send content ID outside of metadata JSON
      String contentId = icebergMetadata.property("nessie.catalog.content-id", null);
      long currentSnapshotId = icebergMetadata.propertyAsLong("current-snapshot-id", 0L);
      int currentSchemaId = icebergMetadata.propertyAsInt("current-schema-id", 0);
      int defaultSpecId = icebergMetadata.propertyAsInt("default-spec-id", 0);
      int defaultSortOrderId = icebergMetadata.propertyAsInt("default-sort-order-id", 0);

      String metadataLocation = response.getRequestUri().toASCIIString();

      IcebergTable table =
          IcebergTable.of(
              response.getRequestUri().toASCIIString(),
              currentSnapshotId,
              currentSchemaId,
              defaultSpecId,
              defaultSortOrderId,
              contentId);

      // TODO Ugly way to set the private `table` field
      try {
        Field field = NessieTableOperations.class.getDeclaredField("table");
        field.setAccessible(true);
        field.set(this, table);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      refreshFromMetadataLocation(metadataLocation, null, 0, location -> icebergMetadata);

    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("No such table '%s' in '%s'", key, reference);
      } else {
        refreshFromMetadataLocation(
            null,
            null,
            2,
            location -> {
              throw new IllegalStateException();
            });
      }
    }
  }

  /** TODO: refactor wrt {@link NessieTableOperations}. */
  private static void maybeThrowSpecializedException(NessieReferenceConflictException ex) {
    // Check if the server returned 'ReferenceConflicts' information
    ReferenceConflicts referenceConflicts = ex.getErrorDetails();
    if (referenceConflicts == null) {
      return;
    }

    // Can only narrow down to a single exception, if there is only one conflict.
    List<Conflict> conflicts = referenceConflicts.conflicts();
    if (conflicts.size() != 1) {
      return;
    }

    Conflict conflict = conflicts.get(0);
    Conflict.ConflictType conflictType = conflict.conflictType();
    if (conflictType != null) {
      switch (conflictType) {
        case NAMESPACE_ABSENT:
          throw new NoSuchNamespaceException(ex, "Namespace does not exist: %s", conflict.key());
        case NAMESPACE_NOT_EMPTY:
          throw new NamespaceNotEmptyException(ex, "Namespace not empty: %s", conflict.key());
        case KEY_DOES_NOT_EXIST:
          throw new NoSuchTableException(ex, "Table or view does not exist: %s", conflict.key());
        case KEY_EXISTS:
          throw new AlreadyExistsException(ex, "Table or view already exists: %s", conflict.key());
        default:
          // Explicit fall-through
          break;
      }
    }
  }
}
