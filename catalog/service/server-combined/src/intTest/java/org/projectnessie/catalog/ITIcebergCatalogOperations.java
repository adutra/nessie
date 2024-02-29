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
package org.projectnessie.catalog;

import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.nessie.NessieCatalogIcebergCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.AbstractCollectionAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
class ITIcebergCatalogOperations {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "value", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 5).build();

  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  private abstract static class Tests {

    @TempDir private static Path warehouseDir;

    private static NessieCatalogIcebergCatalog catalog;

    private final boolean sendUpdatesToServer;
    private final TableIdentifier tableIdentifier;

    private Tests(boolean sendUpdatesToServer) {
      this.sendUpdatesToServer = sendUpdatesToServer;
      this.tableIdentifier = TableIdentifier.of("server-" + sendUpdatesToServer, "test-table");
    }

    @BeforeEach
    void createCatalog() {
      if (catalog != null) {
        return;
      }

      int catalogServerPort = Integer.getInteger("quarkus.http.port");
      catalog = new NessieCatalogIcebergCatalog();
      catalog.setConf(new Configuration());
      catalog.initialize(
          "test-catalog-operations",
          ImmutableMap.<String, String>builder()
              .put("uri", format("http://127.0.0.1:%d/api/v2", catalogServerPort))
              .put("client-api-version", "2")
              .put("warehouse", warehouseDir.toUri().toASCIIString())
              .put("send-updates-to-server", String.valueOf(sendUpdatesToServer))
              .build());
    }

    @AfterAll
    static void closeCatalog() throws IOException {
      if (catalog != null) {
        catalog.close();
        catalog = null;
      }
    }

    @Test
    @Order(100)
    void createNamespace() {
      catalog.createNamespace(tableIdentifier.namespace());
    }

    @Test
    @Order(200)
    void createTable() {

      Table table = catalog.createTable(tableIdentifier, SCHEMA, PARTITION_SPEC);

      assertThat(
              listFiles(new File(URI.create(table.location()).normalize().getPath()), null, true))
          .anySatisfy(
              l -> {
                assertThat(l.getName()).startsWith("00000-");
                assertThat(l.getName()).endsWith(".metadata.json");
              });
    }

    private PartitionKey partitionKey(Table table, long id) {
      GenericRecord key = GenericRecord.create(SCHEMA);
      key.set(1, id);

      PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
      partitionKey.partition(key);
      return partitionKey;
    }

    private DataFile dataFile(Table table, long id) {
      return DataFiles.builder(table.spec())
          .withFormat(FileFormat.PARQUET)
          .withInputFile(Files.localInput(format("data-%d.parquet", id)))
          .withRecordCount(0)
          .withPartition(partitionKey(table, 1))
          .build();
    }

    private AbstractCollectionAssert<?, Collection<? extends File>, File, ObjectAssert<File>>
        assertThatMetadataFiles(Table table) {
      return assertThat(
              listFiles(new File(URI.create(table.location()).normalize().getPath()), null, true))
          .filteredOn(f -> f.getName().endsWith(".metadata.json"));
    }

    @Test
    @Order(301)
    void append1() throws IOException {
      Table table = catalog.loadTable(tableIdentifier);
      table.newAppend().appendFile(dataFile(table, 1)).commit();
      assertThatMetadataFiles(table).hasSize(1 + 1); // 1 from create table
      assertThat(dataFiles(table)).extracting(ContentFile::path).containsExactly("data-1.parquet");
    }

    @Test
    @Order(302)
    void append2() throws IOException {
      Table table = catalog.loadTable(tableIdentifier);
      table.newAppend().appendFile(dataFile(table, 2)).commit();
      assertThatMetadataFiles(table).hasSize(1 + 2); // 1 from create table
      assertThat(dataFiles(table))
          .extracting(ContentFile::path)
          .containsExactlyInAnyOrder("data-1.parquet", "data-2.parquet");
    }

    @Test
    @Order(400)
    void refresh() {
      Table table = catalog.loadTable(tableIdentifier);
      table.refresh();
    }

    @Test
    @Order(501)
    void deleteFiles1() throws IOException {
      Table table = catalog.loadTable(tableIdentifier);
      table.newDelete().deleteFile("data-1.parquet").commit();
      table.refresh();
      assertThatMetadataFiles(table).hasSize(4); // 1 create, 2 appends, 1 delete
      assertThat(dataFiles(table))
          .extracting(ContentFile::path)
          .containsExactlyInAnyOrder("data-2.parquet");
    }

    private List<DataFile> dataFiles(Table table) throws IOException {
      List<DataFile> dataFiles = new ArrayList<>();
      FileIO io = table.io();
      Map<Integer, PartitionSpec> specsById =
          ((HasTableOperations) table).operations().current().specsById();
      for (ManifestFile manifest : table.currentSnapshot().dataManifests(io)) {
        try (CloseableIterable<DataFile> files = ManifestFiles.read(manifest, io, specsById)) {
          files.forEach(dataFiles::add);
        }
      }

      assertThat(dataFiles)
          .extracting(DataFile::content, DataFile::format)
          .allSatisfy(t -> assertThat(t).isEqualTo(tuple(FileContent.DATA, FileFormat.PARQUET)));

      return dataFiles;
    }
  }

  @Nested
  class ClientSideUpdates extends Tests {
    private ClientSideUpdates() {
      super(false);
    }
  }

  @Nested
  class ServerSideUpdates extends Tests {
    private ServerSideUpdates() {
      super(true);
    }
  }
}
