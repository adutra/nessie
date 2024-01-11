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
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.error.NessieNotFoundException;
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
  private final URI baseUri;

  public NessieCatalogTableOperations(
      ContentKey key,
      NessieIcebergClient client,
      FileIO fileIO,
      Map<String, String> catalogOptions) {
    super(key, client, fileIO, catalogOptions);

    this.client = client;
    this.key = key;

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
  protected void doRefresh() {
    try {
      client.refresh();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Failed to refresh as ref '%s' " + "is no longer valid.", client.getRef().getName()),
          e);
    }

    Reference reference = client.getRef().getReference();
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
      System.err.println("IcebergTable: " + table);

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
}
