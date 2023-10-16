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
package org.projectnessie.catalog.service.rest;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.api.rest.spec.NessieCatalogService;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

@RequestScoped
public class CatalogTransportResource implements NessieCatalogService {

  private final CatalogService catalogService;

  public CatalogTransportResource() {
    this(null);
  }

  @Inject
  public CatalogTransportResource(CatalogService catalogService) {
    this.catalogService = catalogService;
  }

  @Override
  public Object tableSnapshot(String ref, ContentKey key, String format, String specVersion)
      throws NessieNotFoundException {
    SnapshotFormat snapshotFormat;
    OptionalInt reqVersion = OptionalInt.empty();

    if (format == null) {
      // No table format specified, return the NessieTableSnapshot as JSON
      snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT;
    } else {
      switch (TableFormat.valueOf(format.toUpperCase(Locale.ROOT))) {
        case ICEBERG:
          // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
          // the request or the one used when the table-metadata was written.
          // TODO Does requesting a table-metadata using another spec-version make any sense?
          // TODO Response should respect the JsonView / spec-version
          // TODO Add a check that the original table format was Iceberg (not Delta)
          snapshotFormat = SnapshotFormat.ICEBERG_TABLE_METADATA;
          if (specVersion != null) {
            reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
          }
          break;
        case DELTA_LAKE:
        default:
          throw new UnsupportedOperationException();
      }
    }

    return snapshotBased(ref, key, snapshotFormat, Optional.empty(), reqVersion);
  }

  @Override
  public Object manifestList(String ref, ContentKey key, String format, String specVersion)
      throws NessieNotFoundException {
    SnapshotFormat snapshotFormat;
    OptionalInt reqVersion = OptionalInt.empty();

    TableFormat tableFormat =
        format != null ? TableFormat.valueOf(format.toUpperCase(Locale.ROOT)) : TableFormat.ICEBERG;

    switch (tableFormat) {
      case ICEBERG:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        snapshotFormat = SnapshotFormat.ICEBERG_MANIFEST_LIST;
        if (specVersion != null) {
          reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
        }
        break;
      case DELTA_LAKE:
      default:
        throw new UnsupportedOperationException();
    }

    return snapshotBased(ref, key, snapshotFormat, Optional.empty(), reqVersion);
  }

  @Override
  public Object manifestFile(
      String ref, ContentKey key, String manifestFile, String format, String specVersion)
      throws NessieNotFoundException {
    SnapshotFormat snapshotFormat;
    NessieId manifestFileId;
    OptionalInt reqVersion = OptionalInt.empty();

    TableFormat tableFormat =
        format != null ? TableFormat.valueOf(format.toUpperCase(Locale.ROOT)) : TableFormat.ICEBERG;

    switch (tableFormat) {
      case ICEBERG:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        manifestFileId = NessieId.nessieIdFromStringBase64(manifestFile);
        snapshotFormat = SnapshotFormat.ICEBERG_MANIFEST_FILE;
        if (specVersion != null) {
          reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
        }
        break;
      case DELTA_LAKE:
      default:
        throw new UnsupportedOperationException();
    }

    return snapshotBased(ref, key, snapshotFormat, Optional.of(manifestFileId), reqVersion);
  }

  private Response snapshotBased(
      String ref,
      ContentKey key,
      SnapshotFormat snapshotFormat,
      Optional<NessieId> manifestFileId,
      OptionalInt reqVersion)
      throws NessieNotFoundException {
    SnapshotResponse snapshot =
        catalogService.retrieveTableSnapshot(ref, key, manifestFileId, snapshotFormat, reqVersion);

    // TODO For REST return an ETag header + cache-relevant fields (consider Nessie commit ID)

    Optional<Object> entity = snapshot.entityObject();
    if (entity.isPresent()) {
      return Response.ok(entity.get())
          .header("Content-Disposition", "attachment; filename=\"" + snapshot.fileName() + "\"")
          .build();
    }

    return Response.ok((StreamingOutput) snapshot::produce)
        .header("Content-Disposition", "attachment; filename=\"" + snapshot.fileName() + "\"")
        .build();
  }
}
