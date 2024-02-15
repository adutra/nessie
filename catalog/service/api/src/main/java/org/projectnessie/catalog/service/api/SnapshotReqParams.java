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
package org.projectnessie.catalog.service.api;

import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface SnapshotReqParams {
  String ref();

  SnapshotFormat snapshotFormat();

  OptionalInt reqVersion();

  Optional<NessieId> manifestFileId();

  static SnapshotReqParams forSnapshotHttpReq(String ref, String format, String specVersion) {
    SnapshotFormat snapshotFormat;
    OptionalInt reqVersion = OptionalInt.empty();

    if (format == null) {
      // No table format specified, return the NessieTableSnapshot as JSON
      snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT;
    } else {
      format = format.toUpperCase(Locale.ROOT);
      SnapshotResultFormat tableFormat = SnapshotResultFormat.valueOf(format);
      switch (tableFormat) {
        case ICEBERG:
          // Return the snapshot as an Iceberg table-metadata using either the spec-version given
          // in
          // the request or the one used when the table-metadata was written.
          // TODO Does requesting a table-metadata using another spec-version make any sense?
          // TODO Response should respect the JsonView / spec-version
          // TODO Add a check that the original table format was Iceberg (not Delta)
          snapshotFormat = SnapshotFormat.ICEBERG_TABLE_METADATA;
          if (specVersion != null) {
            reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
          }
          break;
        case ICEBERG_IMPORTED:
          // Return the snapshot as an Iceberg table-metadata using either the spec-version given
          // in
          // the request or the one used when the table-metadata was written.
          // TODO Does requesting a table-metadata using another spec-version make any sense?
          // TODO Response should respect the JsonView / spec-version
          // TODO Add a check that the original table format was Iceberg (not Delta)
          snapshotFormat = SnapshotFormat.ICEBERG_TABLE_METADATA_IMPORTED;
          if (specVersion != null) {
            reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
          }
          break;
        case NESSIE:
          snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT;
          break;
        case NESSIE_NO_MANIFEST:
          snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT_NO_MANIFESTS;
          break;
        case DELTA_LAKE:
        default:
          throw new UnsupportedOperationException();
      }
    }

    return ImmutableSnapshotReqParams.of(ref, snapshotFormat, reqVersion, Optional.empty());
  }

  static SnapshotReqParams forManifestListHttpReq(String ref, String format, String specVersion) {
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
        snapshotFormat = SnapshotFormat.ICEBERG_MANIFEST_LIST_IMPORTED;
        if (specVersion != null) {
          reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
        }
        break;
      case DELTA_LAKE:
      default:
        throw new UnsupportedOperationException();
    }

    return ImmutableSnapshotReqParams.of(ref, snapshotFormat, reqVersion, Optional.empty());
  }

  static SnapshotReqParams forManifestFileHttpReq(
      String ref, String format, String specVersion, String manifestFile) {
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

        // Strip file extension, if present. Need to append ".avro" to pass Iceberg's file format
        // checks.
        int dotIdx = manifestFile.lastIndexOf('.');
        if (dotIdx != -1) {
          manifestFile = manifestFile.substring(0, dotIdx);
        }

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

    return ImmutableSnapshotReqParams.of(
        ref, snapshotFormat, reqVersion, Optional.of(manifestFileId));
  }

  static SnapshotReqParams forDataFile(String ref, SnapshotFormat snapshotFormat) {
    return ImmutableSnapshotReqParams.of(
        ref, snapshotFormat, OptionalInt.empty(), Optional.empty());
  }
}
