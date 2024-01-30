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
package org.projectnessie.catalog.service.server.tests;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateMetadataWithManifestList;
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.model.CommitMeta.fromMessage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.file.SeekableByteArrayInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;

public class AbstractNessieCoreTests {

  @Test
  public void nessieApiWorks() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    try (NessieApiV2 api =
        createClientBuilderFromSystemSettings()
            .withUri(format("http://127.0.0.1:%d/api/v2", catalogServerPort))
            .build(NessieApiV2.class)) {

      assertThat(api.getConfig().getDefaultBranch()).isEqualTo("main");
    }
  }

  @Test
  public void concurrentImportDemo(@TempDir Path tempDir) throws Exception {
    var tableMetadataLocation = generateMetadataWithManifestList(tempDir);

    var tableName = "concurrentImportDemo";

    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    try (NessieApiV2 api =
        createClientBuilderFromSystemSettings()
            .withUri(format("http://127.0.0.1:%d/api/v2", catalogServerPort))
            .build(NessieApiV2.class)) {

      var committed =
          api.commitMultipleOperations()
              .commitMeta(fromMessage("a table named " + tableName))
              .operation(
                  Operation.Put.of(
                      ContentKey.of(tableName), IcebergTable.of(tableMetadataLocation, 1, 0, 0, 0)))
              .branch(api.getDefaultBranch())
              .commitWithResponse();

      var baseUri =
          api.unwrapClient(org.projectnessie.client.http.HttpClient.class)
              .orElseThrow()
              .getBaseUri();
      if (baseUri.getPath().endsWith("/")) {
        baseUri = baseUri.resolve("..");
      }
      baseUri = baseUri.resolve("../catalog/v1/");
      var snapshotUri = baseUri.resolve("trees/main/snapshot/" + tableName);

      var httpClient =
          Vertx.vertx()
              .createHttpClient(
                  new HttpClientOptions().setMaxPoolSize(1000).setHttp2MaxPoolSize(1000));
      try {
        var requests = new ArrayList<Future<String>>();
        for (int i = 0; i < 1; i++) {
          var req = httpRequest(httpClient, snapshotUri).map(Buffer::toString);
          requests.add(req);
        }
        Future.all(requests).toCompletionStage().toCompletableFuture().get(1000, SECONDS);

        var firstBody = requests.get(0).result().toString();
        assertThat(requests).extracting(Future::result).allMatch(firstBody::equals);
      } finally {
        httpClient.close().toCompletionStage().toCompletableFuture().get(100, SECONDS);
      }
    }
  }

  @Test
  public void checkTableMetadataManifestListManifestFiles(@TempDir Path tempDir) throws Exception {
    var tableMetadataLocation = generateMetadataWithManifestList(tempDir);

    var tableName = "checkTableMetadataManifestListManifestFiles";

    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    try (NessieApiV2 api =
        createClientBuilderFromSystemSettings()
            .withUri(format("http://127.0.0.1:%d/api/v2", catalogServerPort))
            .build(NessieApiV2.class)) {

      var committed =
          api.commitMultipleOperations()
              .commitMeta(fromMessage("a table named " + tableName))
              .operation(
                  Operation.Put.of(
                      ContentKey.of(tableName), IcebergTable.of(tableMetadataLocation, 1, 0, 0, 0)))
              .branch(api.getDefaultBranch())
              .commitWithResponse();

      var baseUri =
          api.unwrapClient(org.projectnessie.client.http.HttpClient.class)
              .orElseThrow()
              .getBaseUri();
      if (baseUri.getPath().endsWith("/")) {
        baseUri = baseUri.resolve("..");
      }
      baseUri = baseUri.resolve("../catalog/v1/");
      var snapshotUri = baseUri.resolve("trees/main/snapshot/" + tableName);
      var manifestListUri = baseUri.resolve("trees/main/manifest-list/" + tableName);

      var httpClient =
          Vertx.vertx()
              .createHttpClient(
                  new HttpClientOptions().setMaxPoolSize(1000).setHttp2MaxPoolSize(1000));
      try {
        var req = httpRequest(httpClient, snapshotUri).map(Buffer::toString);
        var tableMetadata = req.toCompletionStage().toCompletableFuture().get(10, SECONDS);

        var listReq = httpRequest(httpClient, manifestListUri).map(Buffer::getBytes);
        var manifestList = listReq.toCompletionStage().toCompletableFuture().get(10, SECONDS);

        List<CompletableFuture<byte[]>> fileReqs = new ArrayList<>();
        try (var listEntryReader =
            IcebergManifestListReader.builder()
                .build()
                .entryReader(new SeekableByteArrayInput(manifestList))) {
          while (listEntryReader.hasNext()) {
            var listEntry = listEntryReader.next();
            System.out.println(listEntry);
            fileReqs.add(
                httpRequest(httpClient, URI.create(listEntry.manifestPath()))
                    .map(Buffer::getBytes)
                    .toCompletionStage()
                    .toCompletableFuture());
          }
        }

        int i = 0;
        for (CompletableFuture<byte[]> fileReq : fileReqs) {
          var manifestFileData = fileReq.get(10, SECONDS);
          try (var manifestReader =
              IcebergManifestFileReader.builder()
                  .build()
                  .entryReader(new SeekableByteArrayInput(manifestFileData))) {
            while (manifestReader.hasNext()) {
              var mfEntry = manifestReader.next();
              System.err.println("data file: " + mfEntry.dataFile().filePath());
            }
          }
        }

      } finally {
        httpClient.close().toCompletionStage().toCompletableFuture().get(100, SECONDS);
      }
    }
  }

  private static Future<Buffer> httpRequest(HttpClient httpClient, URI snapshotUri) {
    return httpClient
        .request(
            HttpMethod.GET,
            snapshotUri.getPort(),
            snapshotUri.getHost(),
            snapshotUri.getRawPath()
                + (snapshotUri.getRawQuery() != null ? "?" + snapshotUri.getRawQuery() : ""))
        .compose(HttpClientRequest::send)
        .map(
            r -> {
              if (r.statusCode() != 200) {
                throw new RuntimeException(
                    "Failed request to "
                        + snapshotUri
                        + " : HTTP/"
                        + r.statusCode()
                        + " "
                        + r.statusMessage());
              }
              return r;
            })
        .compose(HttpClientResponse::body);
  }
}
